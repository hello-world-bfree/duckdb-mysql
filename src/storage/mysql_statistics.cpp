#include "storage/mysql_statistics.hpp"
#include "mysql_connection.hpp"
#include "mysql_result.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"

namespace duckdb {

bool MySQLIndexInfo::CoversColumns(const vector<string> &required_columns) const {
	if (required_columns.empty()) {
		return false;
	}
	for (const auto &req_col : required_columns) {
		bool found = false;
		for (const auto &idx_col : columns) {
			if (StringUtil::CIEquals(req_col, idx_col)) {
				found = true;
				break;
			}
		}
		if (!found) {
			return false;
		}
	}
	return true;
}

bool MySQLIndexInfo::IsLeadingColumn(const string &column) const {
	if (columns.empty()) {
		return false;
	}
	return StringUtil::CIEquals(columns[0], column);
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindBestIndex(const vector<string> &required_columns) const {
	if (required_columns.empty()) {
		return nullptr;
	}
	optional_ptr<const MySQLIndexInfo> best_match = nullptr;
	idx_t best_match_columns = 0;

	for (const auto &index : indexes) {
		if (!index.IsLeadingColumn(required_columns[0])) {
			continue;
		}
		idx_t matched_columns = 0;
		for (idx_t i = 0; i < required_columns.size() && i < index.columns.size(); i++) {
			if (StringUtil::CIEquals(required_columns[i], index.columns[i])) {
				matched_columns++;
			} else {
				break;
			}
		}
		if (matched_columns > best_match_columns) {
			best_match_columns = matched_columns;
			best_match = &index;
		}
		if (index.is_primary && matched_columns >= best_match_columns && matched_columns > 0) {
			return &index;
		}
	}
	return best_match;
}

optional_ptr<MySQLIndexInfo> MySQLTableStats::FindBestIndex(const vector<string> &required_columns) {
	auto result = const_cast<const MySQLTableStats *>(this)->FindBestIndex(required_columns);
	return const_cast<MySQLIndexInfo *>(result.get());
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindIndexForColumn(const string &column) const {
	for (const auto &index : indexes) {
		if (index.IsLeadingColumn(column)) {
			return &index;
		}
	}
	return nullptr;
}

optional_ptr<MySQLIndexInfo> MySQLTableStats::FindIndexForColumn(const string &column) {
	auto result = const_cast<const MySQLTableStats *>(this)->FindIndexForColumn(column);
	return const_cast<MySQLIndexInfo *>(result.get());
}

bool MySQLTableStats::HasIndexOnColumn(const string &column) const {
	return FindIndexForColumn(column) != nullptr;
}

optional_ptr<const MySQLHistogram> MySQLTableStats::GetHistogram(const string &column) const {
	auto it = histograms.find(column);
	if (it != histograms.end()) {
		return &it->second;
	}
	return nullptr;
}

optional_ptr<const MySQLIndexInfo> MySQLTableStats::FindCoveringIndex(const vector<string> &select_columns,
                                                                      const vector<string> &filter_columns) const {
	vector<string> all_columns;
	all_columns.reserve(select_columns.size() + filter_columns.size());

	for (const auto &col : select_columns) {
		all_columns.push_back(col);
	}
	for (const auto &col : filter_columns) {
		bool already_added = false;
		for (const auto &existing : all_columns) {
			if (StringUtil::CIEquals(existing, col)) {
				already_added = true;
				break;
			}
		}
		if (!already_added) {
			all_columns.push_back(col);
		}
	}

	if (all_columns.empty()) {
		return nullptr;
	}

	for (const auto &index : indexes) {
		if (index.CoversColumns(all_columns)) {
			bool has_leading_filter = false;
			for (const auto &filter_col : filter_columns) {
				if (index.IsLeadingColumn(filter_col)) {
					has_leading_filter = true;
					break;
				}
			}
			if (has_leading_filter) {
				return &index;
			}
		}
	}

	for (const auto &index : indexes) {
		if (index.CoversColumns(all_columns)) {
			return &index;
		}
	}

	return nullptr;
}

vector<string> MySQLPartitionInfo::GetMatchingPartitions(const Value &filter_value, ExpressionType comparison) const {
	vector<string> matching;

	if (!IsRangeOrList() || partitions.empty()) {
		return matching;
	}

	if (type == MySQLPartitionType::RANGE || type == MySQLPartitionType::RANGE_COLUMNS) {
		for (const auto &part : partitions) {
			if (part.description.empty() || part.is_subpartition) {
				continue;
			}
			if (part.description == "MAXVALUE") {
				if (comparison == ExpressionType::COMPARE_GREATERTHAN ||
				    comparison == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
					matching.push_back(part.partition_name);
				} else if (comparison == ExpressionType::COMPARE_EQUAL) {
					matching.push_back(part.partition_name);
				}
				continue;
			}
			try {
				Value bound = Value(std::stoll(part.description));
				switch (comparison) {
				case ExpressionType::COMPARE_LESSTHAN:
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					if (filter_value < bound || filter_value == bound) {
						matching.push_back(part.partition_name);
					}
					break;
				case ExpressionType::COMPARE_EQUAL:
					if (filter_value < bound) {
						matching.push_back(part.partition_name);
					}
					break;
				case ExpressionType::COMPARE_GREATERTHAN:
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					if (filter_value >= bound || filter_value == bound) {
						matching.push_back(part.partition_name);
					}
					break;
				default:
					return {};
				}
			} catch (const std::exception &) {
				return {};
			}
		}
	} else if (type == MySQLPartitionType::LIST || type == MySQLPartitionType::LIST_COLUMNS) {
		if (comparison != ExpressionType::COMPARE_EQUAL) {
			return {};
		}
		string filter_str = filter_value.ToString();
		for (const auto &part : partitions) {
			if (part.description.empty() || part.is_subpartition) {
				continue;
			}
			if (part.description.find(filter_str) != string::npos) {
				matching.push_back(part.partition_name);
			}
		}
	}

	return matching;
}

double MySQLHistogram::EstimateSelectivity(ExpressionType comparison, const Value &value, idx_t total_rows) const {
	if (buckets.empty() || bucket_bounds.empty()) {
		return -1.0;
	}

	if (histogram_type == "singleton") {
		for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
			if (bucket_bounds[i] == value) {
				double prev_freq = (i > 0) ? buckets[i - 1].cumulative_frequency : 0.0;
				return buckets[i].cumulative_frequency - prev_freq;
			}
		}
		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return 0.0;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0;
		default:
			break;
		}
	}

	if (histogram_type == "equi-height") {
		idx_t bucket_idx = 0;
		for (idx_t i = 0; i < bucket_bounds.size(); i++) {
			if (value <= bucket_bounds[i]) {
				bucket_idx = i;
				break;
			}
			bucket_idx = i;
		}

		double bucket_freq = (bucket_idx < buckets.size())
		                         ? (bucket_idx > 0 ? buckets[bucket_idx].cumulative_frequency -
		                                                 buckets[bucket_idx - 1].cumulative_frequency
		                                           : buckets[bucket_idx].cumulative_frequency)
		                         : 0.0;
		double bucket_distinct = (bucket_idx < buckets.size() && buckets[bucket_idx].num_distinct > 0)
		                             ? buckets[bucket_idx].num_distinct
		                             : 1;

		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return bucket_freq / bucket_distinct;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0 - (bucket_freq / bucket_distinct);
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return (bucket_idx < buckets.size()) ? buckets[bucket_idx].cumulative_frequency : 1.0;
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 1.0 - ((bucket_idx < buckets.size()) ? buckets[bucket_idx].cumulative_frequency : 1.0);
		default:
			return -1.0;
		}
	}

	return -1.0;
}

double MySQLHistogram::EstimateRangeSelectivity(const Value &low, const Value &high, bool low_inclusive,
                                                bool high_inclusive) const {
	if (buckets.empty() || bucket_bounds.empty()) {
		return -1.0;
	}

	double low_freq = 0.0;
	double high_freq = 1.0;

	for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
		if (low <= bucket_bounds[i]) {
			low_freq = (i > 0) ? buckets[i - 1].cumulative_frequency : 0.0;
			break;
		}
	}

	for (idx_t i = 0; i < bucket_bounds.size() && i < buckets.size(); i++) {
		if (high <= bucket_bounds[i]) {
			high_freq = buckets[i].cumulative_frequency;
			break;
		}
	}

	return std::max(0.0, high_freq - low_freq);
}

MySQLStatisticsCollector::MySQLStatisticsCollector(MySQLConnection &connection) : connection_(connection) {
}

double MySQLStatisticsCollector::ComputeStalenessScore(const MySQLTableStats &stats) {
	double score = 0.0;

	if (stats.has_update_time) {
		auto now_us = Timestamp::GetCurrentTimestamp().value;
		int64_t age_us = now_us - stats.update_time_epoch_us;
		int64_t days = age_us / (1000000LL * 60 * 60 * 24);
		if (days > 30) {
			score += 0.3;
		} else if (days > 7) {
			score += 0.1;
		}
	} else {
		score += 0.2;
	}

	if (stats.histograms.empty() && stats.estimated_row_count > 10000) {
		score += 0.2;
	}

	for (const auto &idx : stats.indexes) {
		if (idx.is_primary && idx.cardinality > stats.estimated_row_count * 3 / 2) {
			score += 0.3;
			break;
		}
	}

	return std::min(score, 1.0);
}

void MySQLStatisticsCollector::DetectVersion() {
	if (version_detected_) {
		return;
	}
	version_detected_ = true;

	auto result = connection_.get().Query(
	    "SELECT @@version AS mysql_version, "
	    "EXISTS(SELECT 1 FROM information_schema.TABLES "
	    "WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMN_STATISTICS') AS has_histogram",
	    MySQLResultStreaming::FORCE_MATERIALIZATION);

	if (result->Exhausted()) {
		return;
	}

	auto &chunk = result->NextChunk();
	if (chunk.size() == 0) {
		return;
	}

	mysql_version_ = chunk.data[0].GetValue(0).ToString();
	has_histogram_support_ = chunk.data[1].GetValue(0).GetValue<bool>();
}

string MySQLStatisticsCollector::GetCacheKey(const string &schema, const string &table) const {
	string key;
	key.reserve(schema.size() + 1 + table.size());
	key += schema;
	key += ".";
	key += table;
	return key;
}

void MySQLStatisticsCollector::ClearCache() {
	stats_cache_.clear();
}

void MySQLStatisticsCollector::ClearCache(const string &schema, const string &table) {
	auto key = GetCacheKey(schema, table);
	stats_cache_.erase(key);
}

void MySQLStatisticsCollector::EnsureFreshStats() {
	if (stats_expiry_set_) {
		return;
	}
	stats_expiry_set_ = true;
	try {
		connection_.get().Query("SET SESSION information_schema_stats_expiry = 0",
		                        MySQLResultStreaming::FORCE_MATERIALIZATION);
	} catch (const std::exception &) {
	}
}

MySQLTableStats MySQLStatisticsCollector::GetTableStats(const string &schema, const string &table) {
	DetectVersion();
	EnsureFreshStats();

	auto cache_key = GetCacheKey(schema, table);
	auto it = stats_cache_.find(cache_key);
	if (it != stats_cache_.end()) {
		auto elapsed = std::chrono::steady_clock::now() - it->second.cached_at;
		auto seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
		if (seconds < STATS_CACHE_TTL_SECONDS) {
			return it->second.stats;
		}
		stats_cache_.erase(it);
	}

	MySQLTableStats stats;
	stats.schema_name = schema;
	stats.table_name = table;

	FetchTableMetadata(schema, table, stats);
	FetchIndexInfo(schema, table, stats);
	FetchInnoDBStats(schema, table, stats);
	FetchColumnStats(schema, table, stats);
	FetchPartitionInfo(schema, table, stats);
	if (has_histogram_support_) {
		FetchHistograms(schema, table, stats);
		if (stats.estimated_row_count > 0) {
			for (const auto &hist_entry : stats.histograms) {
				const auto &hist = hist_entry.second;
				if (hist.null_count > 0) {
					double null_frac =
					    static_cast<double>(hist.null_count) / static_cast<double>(stats.estimated_row_count);
					stats.column_null_fraction[hist.column_name] = std::min(null_frac, 1.0);
				}
			}
		}
	}

	if (stats_cache_.size() >= MAX_STATS_CACHE_SIZE) {
		auto oldest = stats_cache_.begin();
		for (auto sit = stats_cache_.begin(); sit != stats_cache_.end(); ++sit) {
			if (sit->second.cached_at < oldest->second.cached_at) {
				oldest = sit;
			}
		}
		stats_cache_.erase(oldest);
	}

	CachedTableStats cached;
	cached.stats = stats;
	cached.cached_at = std::chrono::steady_clock::now();
	stats_cache_[cache_key] = std::move(cached);
	return stats;
}

void MySQLStatisticsCollector::FetchTableMetadata(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH, ENGINE, "
	               "IF(CREATE_OPTIONS LIKE '%partitioned%', 1, 0) AS is_partitioned, "
	               "IF(TABLE_TYPE = 'VIEW', 1, 0) AS is_view, "
	               "ROW_FORMAT, UPDATE_TIME "
	               "FROM information_schema.TABLES "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	auto &chunk = result->NextChunk();
	if (chunk.size() == 0) {
		return;
	}

	auto row_count_val = chunk.data[0].GetValue(0);
	if (!row_count_val.IsNull()) {
		int64_t row_count = row_count_val.GetValue<int64_t>();
		stats.estimated_row_count = (row_count > 0) ? static_cast<idx_t>(row_count) : 0;
	}

	auto avg_row_val = chunk.data[1].GetValue(0);
	if (!avg_row_val.IsNull()) {
		int64_t avg_row_length = avg_row_val.GetValue<int64_t>();
		stats.avg_row_length = (avg_row_length > 0) ? static_cast<idx_t>(avg_row_length) : 0;
	}

	auto data_len_val = chunk.data[2].GetValue(0);
	if (!data_len_val.IsNull()) {
		int64_t data_length = data_len_val.GetValue<int64_t>();
		stats.data_length = (data_length > 0) ? static_cast<idx_t>(data_length) : 0;
	}

	auto index_len_val = chunk.data[3].GetValue(0);
	if (!index_len_val.IsNull()) {
		int64_t index_length = index_len_val.GetValue<int64_t>();
		stats.index_length = (index_length > 0) ? static_cast<idx_t>(index_length) : 0;
	}

	auto engine_val = chunk.data[4].GetValue(0);
	if (!engine_val.IsNull()) {
		stats.engine = engine_val.ToString();
	}

	auto partitioned_val = chunk.data[5].GetValue(0);
	if (!partitioned_val.IsNull()) {
		stats.is_partitioned = partitioned_val.GetValue<int64_t>() != 0;
	}

	auto view_val = chunk.data[6].GetValue(0);
	if (!view_val.IsNull()) {
		stats.is_view = view_val.GetValue<int64_t>() != 0;
	}

	auto row_format_val = chunk.data[7].GetValue(0);
	if (!row_format_val.IsNull()) {
		stats.row_format = row_format_val.ToString();
	}

	auto update_time_val = chunk.data[8].GetValue(0);
	if (!update_time_val.IsNull()) {
		auto ts = update_time_val.GetValue<timestamp_t>();
		stats.update_time_epoch_us = Timestamp::GetEpochMicroSeconds(ts);
		stats.has_update_time = true;
	}

	stats.staleness_score = ComputeStalenessScore(stats);
}

void MySQLStatisticsCollector::FetchInnoDBStats(const string &schema, const string &table, MySQLTableStats &stats) {
	if (!StringUtil::CIEquals(stats.engine, "InnoDB")) {
		return;
	}

	try {
		string table_stats_query = "SELECT n_rows, clustered_index_size, sum_of_other_index_sizes "
		                           "FROM mysql.innodb_table_stats "
		                           "WHERE database_name = ? AND table_name = ?";
		vector<Value> params;
		params.push_back(Value(schema));
		params.push_back(Value(table));

		auto result = connection_.get().Query(table_stats_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			if (chunk.size() > 0) {
				auto n_rows_val = chunk.data[0].GetValue(0);
				if (!n_rows_val.IsNull()) {
					int64_t n_rows = n_rows_val.GetValue<int64_t>();
					stats.innodb_row_count = (n_rows > 0) ? static_cast<idx_t>(n_rows) : 0;
					if (stats.innodb_row_count > 0) {
						stats.estimated_row_count = stats.innodb_row_count;
					}
				}
				auto clustered_size_val = chunk.data[1].GetValue(0);
				if (!clustered_size_val.IsNull()) {
					int64_t cs = clustered_size_val.GetValue<int64_t>();
					stats.clustered_index_size_pages = (cs > 0) ? static_cast<idx_t>(cs) : 0;
				}
				stats.has_innodb_stats = true;
			}
		}
	} catch (const std::exception &) {
		return;
	}

	try {
		for (auto &index : stats.indexes) {
			string idx_name = index.is_primary ? "PRIMARY" : index.index_name;

			string index_stats_query = "SELECT stat_name, stat_value "
			                           "FROM mysql.innodb_index_stats "
			                           "WHERE database_name = ? AND table_name = ? AND index_name = ? "
			                           "AND (stat_name LIKE 'n_diff_pfx%' OR stat_name = 'size')";
			vector<Value> params;
			params.push_back(Value(schema));
			params.push_back(Value(table));
			params.push_back(Value(idx_name));

			auto result =
			    connection_.get().Query(index_stats_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
			if (result->Exhausted()) {
				continue;
			}

			while (!result->Exhausted()) {
				auto &chunk = result->NextChunk();
				for (idx_t row = 0; row < chunk.size(); row++) {
					string stat_name = chunk.data[0].GetValue(row).ToString();
					int64_t stat_value = chunk.data[1].GetValue(row).GetValue<int64_t>();

					if (stat_name == "size") {
						index.index_size_pages = (stat_value > 0) ? static_cast<idx_t>(stat_value) : 0;
					} else if (StringUtil::StartsWith(stat_name, "n_diff_pfx")) {
						string num_str = stat_name.substr(10);
						try {
							idx_t prefix_idx = static_cast<idx_t>(std::stoull(num_str));
							if (prefix_idx > 0) {
								idx_t arr_idx = prefix_idx - 1;
								if (index.prefix_distinct_counts.size() <= arr_idx) {
									index.prefix_distinct_counts.resize(arr_idx + 1, 0);
								}
								index.prefix_distinct_counts[arr_idx] =
								    (stat_value > 0) ? static_cast<idx_t>(stat_value) : 0;
							}
						} catch (const std::exception &) {
						}
					}
				}
			}

			if (!index.prefix_distinct_counts.empty() && index.prefix_distinct_counts[0] > 0) {
				if (!index.columns.empty()) {
					stats.column_distinct_count[index.columns[0]] = index.prefix_distinct_counts[0];
				}
			}
		}
	} catch (const std::exception &) {
	}
}

void MySQLStatisticsCollector::FetchIndexInfo(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, IFNULL(CARDINALITY, 0), "
	               "INDEX_TYPE, IF(INDEX_NAME = 'PRIMARY', 1, 0) AS is_primary, IF(NON_UNIQUE = 0, 1, 0) AS is_unique "
	               "FROM information_schema.STATISTICS "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
	               "ORDER BY INDEX_NAME, SEQ_IN_INDEX";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	case_insensitive_map_t<MySQLIndexInfo> index_map;

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string index_name = chunk.data[0].GetValue(row).ToString();
			string column_name = chunk.data[2].GetValue(row).ToString();
			int64_t cardinality_val = chunk.data[3].GetValue(row).GetValue<int64_t>();
			idx_t cardinality = (cardinality_val > 0) ? static_cast<idx_t>(cardinality_val) : 0;
			string index_type = chunk.data[4].GetValue(row).ToString();
			bool is_primary = chunk.data[5].GetValue(row).GetValue<int64_t>() != 0;
			bool is_unique = chunk.data[6].GetValue(row).GetValue<int64_t>() != 0;

			auto it = index_map.find(index_name);
			if (it == index_map.end()) {
				MySQLIndexInfo info;
				info.index_name = index_name;
				info.index_type = index_type;
				info.is_primary = is_primary;
				info.is_unique = is_unique;
				info.cardinality = cardinality;
				info.columns.push_back(column_name);
				index_map[index_name] = std::move(info);
			} else {
				it->second.columns.push_back(column_name);
				if (cardinality > it->second.cardinality) {
					it->second.cardinality = cardinality;
				}
			}
		}
	}

	for (auto &entry : index_map) {
		stats.indexes.push_back(std::move(entry.second));
	}
}

void MySQLStatisticsCollector::FetchPartitionInfo(const string &schema, const string &table, MySQLTableStats &stats) {
	if (!stats.is_partitioned) {
		return;
	}

	string method_query = "SELECT PARTITION_METHOD, PARTITION_EXPRESSION "
	                      "FROM information_schema.PARTITIONS "
	                      "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL "
	                      "LIMIT 1";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto method_result = connection_.get().Query(method_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (method_result->Exhausted()) {
		return;
	}

	auto &method_chunk = method_result->NextChunk();
	if (method_chunk.size() == 0) {
		return;
	}

	auto method_val = method_chunk.data[0].GetValue(0);
	if (!method_val.IsNull()) {
		string method = method_val.ToString();
		if (method == "RANGE") {
			stats.partition_info.type = MySQLPartitionType::RANGE;
		} else if (method == "RANGE COLUMNS") {
			stats.partition_info.type = MySQLPartitionType::RANGE_COLUMNS;
		} else if (method == "LIST") {
			stats.partition_info.type = MySQLPartitionType::LIST;
		} else if (method == "LIST COLUMNS") {
			stats.partition_info.type = MySQLPartitionType::LIST_COLUMNS;
		} else if (method == "HASH") {
			stats.partition_info.type = MySQLPartitionType::HASH;
		} else if (method == "KEY") {
			stats.partition_info.type = MySQLPartitionType::KEY;
		}
	}

	auto expr_val = method_chunk.data[1].GetValue(0);
	if (!expr_val.IsNull()) {
		string expr = expr_val.ToString();
		if (stats.partition_info.type == MySQLPartitionType::RANGE_COLUMNS ||
		    stats.partition_info.type == MySQLPartitionType::LIST_COLUMNS ||
		    stats.partition_info.type == MySQLPartitionType::KEY) {
			auto columns = StringUtil::Split(expr, ",");
			for (auto &col : columns) {
				StringUtil::Trim(col);
				if (!col.empty() && col.front() == '`' && col.back() == '`') {
					col = col.substr(1, col.size() - 2);
				}
				stats.partition_info.partition_columns.push_back(col);
			}
		} else {
			size_t start = expr.find('(');
			size_t end = expr.rfind(')');
			if (start != string::npos && end != string::npos && start < end) {
				string col = expr.substr(start + 1, end - start - 1);
				StringUtil::Trim(col);
				if (!col.empty() && col.front() == '`' && col.back() == '`') {
					col = col.substr(1, col.size() - 2);
				}
				stats.partition_info.partition_columns.push_back(col);
			}
		}
	}

	string partition_query = "SELECT PARTITION_NAME, SUBPARTITION_NAME, TABLE_ROWS, PARTITION_DESCRIPTION "
	                         "FROM information_schema.PARTITIONS "
	                         "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL "
	                         "ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION";

	auto partition_result =
	    connection_.get().Query(partition_query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (partition_result->Exhausted()) {
		return;
	}

	while (!partition_result->Exhausted()) {
		auto &chunk = partition_result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			MySQLPartitionDetail detail;

			auto part_name_val = chunk.data[0].GetValue(row);
			if (!part_name_val.IsNull()) {
				detail.partition_name = part_name_val.ToString();
			}

			auto subpart_name_val = chunk.data[1].GetValue(row);
			if (!subpart_name_val.IsNull()) {
				detail.subpartition_name = subpart_name_val.ToString();
				detail.is_subpartition = true;
			}

			auto rows_val = chunk.data[2].GetValue(row);
			if (!rows_val.IsNull()) {
				int64_t rows = rows_val.GetValue<int64_t>();
				detail.estimated_rows = (rows > 0) ? static_cast<idx_t>(rows) : 0;
			}

			auto desc_val = chunk.data[3].GetValue(row);
			if (!desc_val.IsNull()) {
				detail.description = desc_val.ToString();
			}

			stats.partition_info.partitions.push_back(std::move(detail));
		}
	}

	stats.partition_info.partition_count = stats.partition_info.partitions.size();
}

void MySQLStatisticsCollector::FetchColumnStats(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT COLUMN_NAME, IS_NULLABLE "
	               "FROM information_schema.COLUMNS "
	               "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	auto result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	if (result->Exhausted()) {
		return;
	}

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string column_name = chunk.data[0].GetValue(row).ToString();
			string is_nullable = chunk.data[1].GetValue(row).ToString();
			stats.column_null_fraction[column_name] = (is_nullable == "YES") ? 0.01 : 0.0;
		}
	}

	for (const auto &index : stats.indexes) {
		if (!index.columns.empty() && index.cardinality != DConstants::INVALID_INDEX) {
			stats.column_distinct_count[index.columns[0]] = index.cardinality;
		}
	}
}

void MySQLStatisticsCollector::FetchHistograms(const string &schema, const string &table, MySQLTableStats &stats) {
	string query = "SELECT COLUMN_NAME, HISTOGRAM "
	               "FROM information_schema.COLUMN_STATISTICS "
	               "WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?";

	vector<Value> params;
	params.push_back(Value(schema));
	params.push_back(Value(table));

	unique_ptr<MySQLResult> result;
	try {
		result = connection_.get().Query(query, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
	} catch (const std::exception &) {
		return;
	}

	if (!result || result->Exhausted()) {
		return;
	}

	while (!result->Exhausted()) {
		auto &chunk = result->NextChunk();
		for (idx_t row = 0; row < chunk.size(); row++) {
			string column_name = chunk.data[0].GetValue(row).ToString();
			string histogram_json = chunk.data[1].GetValue(row).ToString();

			MySQLHistogram hist;
			hist.column_name = column_name;

			size_t type_pos = histogram_json.find("\"histogram-type\"");
			if (type_pos != string::npos) {
				size_t colon = histogram_json.find(':', type_pos);
				size_t quote_start = histogram_json.find('"', colon);
				size_t quote_end = histogram_json.find('"', quote_start + 1);
				if (quote_start != string::npos && quote_end != string::npos) {
					hist.histogram_type = histogram_json.substr(quote_start + 1, quote_end - quote_start - 1);
				}
			}

			size_t buckets_pos = histogram_json.find("\"number-of-buckets-specified\"");
			if (buckets_pos != string::npos) {
				size_t colon = histogram_json.find(':', buckets_pos);
				size_t num_start = colon + 1;
				while (num_start < histogram_json.size() &&
				       (histogram_json[num_start] == ' ' || histogram_json[num_start] == ':')) {
					num_start++;
				}
				size_t num_end = num_start;
				while (num_end < histogram_json.size() && histogram_json[num_end] >= '0' &&
				       histogram_json[num_end] <= '9') {
					num_end++;
				}
				if (num_end > num_start) {
					try {
						hist.num_buckets = std::stoull(histogram_json.substr(num_start, num_end - num_start));
					} catch (const std::exception &) {
					}
				}
			}

			size_t sampling_pos = histogram_json.find("\"sampling-rate\"");
			if (sampling_pos != string::npos) {
				size_t colon = histogram_json.find(':', sampling_pos);
				size_t num_start = colon + 1;
				while (num_start < histogram_json.size() &&
				       (histogram_json[num_start] == ' ' || histogram_json[num_start] == ':')) {
					num_start++;
				}
				size_t num_end = num_start;
				while (num_end < histogram_json.size() &&
				       ((histogram_json[num_end] >= '0' && histogram_json[num_end] <= '9') ||
				        histogram_json[num_end] == '.')) {
					num_end++;
				}
				if (num_end > num_start) {
					try {
						hist.sampling_rate = std::stod(histogram_json.substr(num_start, num_end - num_start));
					} catch (const std::exception &) {
					}
				}
			}

			size_t null_pos = histogram_json.find("\"null-values\"");
			if (null_pos != string::npos) {
				size_t colon = histogram_json.find(':', null_pos);
				size_t num_start = colon + 1;
				while (num_start < histogram_json.size() &&
				       (histogram_json[num_start] == ' ' || histogram_json[num_start] == ':')) {
					num_start++;
				}
				size_t num_end = num_start;
				while (num_end < histogram_json.size() &&
				       ((histogram_json[num_end] >= '0' && histogram_json[num_end] <= '9') ||
				        histogram_json[num_end] == '.')) {
					num_end++;
				}
				if (num_end > num_start) {
					try {
						double null_frac = std::stod(histogram_json.substr(num_start, num_end - num_start));
						hist.null_count =
						    static_cast<idx_t>(null_frac * static_cast<double>(stats.estimated_row_count));
					} catch (const std::exception &) {
					}
				}
			}

			size_t buckets_start = histogram_json.find("\"buckets\"");
			if (buckets_start != string::npos) {
				size_t arr_start = histogram_json.find('[', buckets_start);
				if (arr_start != string::npos) {
					int bracket_depth = 1;
					size_t pos = arr_start + 1;
					while (pos < histogram_json.size() && bracket_depth > 0) {
						if (histogram_json[pos] == '[') {
							bracket_depth++;
							if (bracket_depth == 2) {
								size_t bucket_start = pos;
								while (pos < histogram_json.size() && bracket_depth >= 2) {
									pos++;
									if (histogram_json[pos] == '[') {
										bracket_depth++;
									} else if (histogram_json[pos] == ']') {
										bracket_depth--;
									}
								}
								string bucket_str = histogram_json.substr(bucket_start, pos - bucket_start + 1);

								vector<string> elements;
								{
									size_t elem_start = 1;
									for (size_t ep = 1; ep < bucket_str.size(); ep++) {
										if (bucket_str[ep] == ',' || bucket_str[ep] == ']') {
											string elem = bucket_str.substr(elem_start, ep - elem_start);
											StringUtil::Trim(elem);
											if (!elem.empty()) {
												elements.push_back(elem);
											}
											elem_start = ep + 1;
										}
									}
								}

								MySQLHistogramBucket bucket;
								if (hist.histogram_type == "singleton" && elements.size() >= 2) {
									string val_str = elements[0];
									if (val_str.front() == '"' && val_str.back() == '"') {
										val_str = val_str.substr(1, val_str.size() - 2);
									}
									try {
										hist.bucket_bounds.push_back(Value(std::stoll(val_str)));
									} catch (...) {
										hist.bucket_bounds.push_back(Value(val_str));
									}
									try {
										bucket.cumulative_frequency = std::stod(elements[1]);
									} catch (const std::exception &) {
									}
								} else if (hist.histogram_type == "equi-height" && elements.size() >= 4) {
									string val_str = elements[0];
									if (val_str.front() == '"' && val_str.back() == '"') {
										val_str = val_str.substr(1, val_str.size() - 2);
									}
									try {
										hist.bucket_bounds.push_back(Value(std::stoll(val_str)));
									} catch (...) {
										hist.bucket_bounds.push_back(Value(val_str));
									}
									try {
										bucket.cumulative_frequency = std::stod(elements[2]);
									} catch (const std::exception &) {
									}
									try {
										bucket.num_distinct = std::stod(elements[3]);
									} catch (const std::exception &) {
									}
								} else if (!elements.empty()) {
									size_t last_idx = elements.size() - 1;
									try {
										bucket.cumulative_frequency = std::stod(elements[last_idx]);
									} catch (const std::exception &) {
									}
									if (!elements.empty()) {
										string val_str = elements[0];
										if (val_str.front() == '"' && val_str.back() == '"') {
											val_str = val_str.substr(1, val_str.size() - 2);
										}
										try {
											hist.bucket_bounds.push_back(Value(std::stoll(val_str)));
										} catch (...) {
											hist.bucket_bounds.push_back(Value(val_str));
										}
									}
								}

								hist.buckets.push_back(bucket);
							}
						} else if (histogram_json[pos] == ']') {
							bracket_depth--;
						}
						pos++;
					}
				}
			}

			if (!hist.buckets.empty()) {
				stats.histograms[column_name] = std::move(hist);
			}
		}
	}
}

MySQLStatisticsCollector::MySQLCostConstants MySQLStatisticsCollector::FetchMySQLCostConstants() {
	MySQLCostConstants constants;

	try {
		auto result = connection_.get().Query("SELECT cost_name, cost_value, default_value "
		                                      "FROM mysql.server_cost",
		                                      MySQLResultStreaming::FORCE_MATERIALIZATION);
		while (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			for (idx_t row = 0; row < chunk.size(); row++) {
				string name = chunk.data[0].GetValue(row).ToString();
				auto cost_val = chunk.data[1].GetValue(row);
				auto default_val = chunk.data[2].GetValue(row);
				double value = 0.0;
				if (!cost_val.IsNull()) {
					value = cost_val.GetValue<double>();
				} else if (!default_val.IsNull()) {
					value = default_val.GetValue<double>();
				}
				if (name == "row_evaluate_cost" && value > 0) {
					constants.row_evaluate_cost = value;
				}
			}
		}
	} catch (const std::exception &) {
		return constants;
	}

	try {
		auto result = connection_.get().Query("SELECT cost_name, cost_value, default_value "
		                                      "FROM mysql.engine_cost",
		                                      MySQLResultStreaming::FORCE_MATERIALIZATION);
		while (!result->Exhausted()) {
			auto &chunk = result->NextChunk();
			for (idx_t row = 0; row < chunk.size(); row++) {
				string name = chunk.data[0].GetValue(row).ToString();
				auto cost_val = chunk.data[1].GetValue(row);
				auto default_val = chunk.data[2].GetValue(row);
				double value = 0.0;
				if (!cost_val.IsNull()) {
					value = cost_val.GetValue<double>();
				} else if (!default_val.IsNull()) {
					value = default_val.GetValue<double>();
				}
				if (name == "io_block_read_cost" && value > 0) {
					constants.io_block_read_cost = value;
				} else if (name == "memory_block_read_cost" && value > 0) {
					constants.memory_block_read_cost = value;
				}
			}
		}
	} catch (const std::exception &) {
		return constants;
	}

	constants.loaded = true;
	return constants;
}

bool MySQLStatisticsCollector::ValidateWithExplain(const string &query, const string &expected_index,
                                                   bool expect_index_scan) {
	try {
		string explain_query = "EXPLAIN FORMAT=JSON " + query;
		auto result = connection_.get().Query(explain_query, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (result->Exhausted()) {
			return true;
		}

		auto &chunk = result->NextChunk();
		if (chunk.size() == 0) {
			return true;
		}

		string explain_json = chunk.data[0].GetValue(0).ToString();

		size_t access_type_pos = explain_json.find("\"access_type\"");
		if (access_type_pos == string::npos) {
			return true;
		}

		size_t colon = explain_json.find(':', access_type_pos);
		size_t quote_start = explain_json.find('"', colon);
		size_t quote_end = explain_json.find('"', quote_start + 1);
		if (quote_start == string::npos || quote_end == string::npos) {
			return true;
		}
		string access_type = explain_json.substr(quote_start + 1, quote_end - quote_start - 1);

		if (expect_index_scan && access_type == "ALL") {
			return false;
		}

		if (!expected_index.empty()) {
			size_t key_pos = explain_json.find("\"key\"");
			if (key_pos != string::npos) {
				size_t kcolon = explain_json.find(':', key_pos);
				size_t kquote_start = explain_json.find('"', kcolon);
				size_t kquote_end = explain_json.find('"', kquote_start + 1);
				if (kquote_start != string::npos && kquote_end != string::npos) {
					string actual_key = explain_json.substr(kquote_start + 1, kquote_end - kquote_start - 1);
					if (!StringUtil::CIEquals(actual_key, expected_index)) {
						return false;
					}
				}
			}
		}
	} catch (const std::exception &) {
	}
	return true;
}

int MySQLStatisticsCollector::GetMajorVersion() const {
	if (mysql_version_.empty()) {
		return 0;
	}
	auto dot = mysql_version_.find('.');
	if (dot == string::npos) {
		return 0;
	}
	try {
		return std::stoi(mysql_version_.substr(0, dot));
	} catch (...) {
		return 0;
	}
}

int MySQLStatisticsCollector::GetMinorVersion() const {
	if (mysql_version_.empty()) {
		return 0;
	}
	auto first_dot = mysql_version_.find('.');
	if (first_dot == string::npos) {
		return 0;
	}
	auto second_dot = mysql_version_.find('.', first_dot + 1);
	if (second_dot == string::npos) {
		return 0;
	}
	try {
		return std::stoi(mysql_version_.substr(first_dot + 1, second_dot - first_dot - 1));
	} catch (...) {
		return 0;
	}
}

bool MySQLStatisticsCollector::SupportsExplainAnalyze() const {
	return GetMajorVersion() > 8 || (GetMajorVersion() == 8 && GetMinorVersion() >= 0);
}

MySQLStatisticsCollector::ExplainAnalyzeResult MySQLStatisticsCollector::RunExplainAnalyze(const string &query) {
	ExplainAnalyzeResult result;
	if (!SupportsExplainAnalyze()) {
		return result;
	}

	try {
		string explain_query = "EXPLAIN ANALYZE FORMAT=JSON " + query;
		auto res = connection_.get().Query(explain_query, MySQLResultStreaming::FORCE_MATERIALIZATION);
		if (res->Exhausted()) {
			return result;
		}

		auto &chunk = res->NextChunk();
		if (chunk.size() == 0) {
			return result;
		}

		string json = chunk.data[0].GetValue(0).ToString();

		size_t rows_pos = json.find("\"rows_examined_per_scan\"");
		if (rows_pos != string::npos) {
			size_t colon = json.find(':', rows_pos);
			size_t num_start = colon + 1;
			while (num_start < json.size() && (json[num_start] == ' ' || json[num_start] == ':')) {
				num_start++;
			}
			size_t num_end = num_start;
			while (num_end < json.size() && json[num_end] >= '0' && json[num_end] <= '9') {
				num_end++;
			}
			if (num_end > num_start) {
				result.actual_rows = static_cast<idx_t>(std::stoull(json.substr(num_start, num_end - num_start)));
			}
		}

		size_t time_pos = json.find("\"actual_time\"");
		if (time_pos == string::npos) {
			time_pos = json.find("\"query_cost\"");
		}
		if (time_pos != string::npos) {
			size_t colon = json.find(':', time_pos);
			size_t num_start = colon + 1;
			while (num_start < json.size() && (json[num_start] == ' ' || json[num_start] == '"')) {
				num_start++;
			}
			size_t num_end = num_start;
			while (num_end < json.size() && ((json[num_end] >= '0' && json[num_end] <= '9') || json[num_end] == '.')) {
				num_end++;
			}
			if (num_end > num_start) {
				result.actual_time_ms = std::stod(json.substr(num_start, num_end - num_start));
			}
		}

		size_t access_pos = json.find("\"access_type\"");
		if (access_pos != string::npos) {
			size_t colon = json.find(':', access_pos);
			size_t q_start = json.find('"', colon);
			size_t q_end = json.find('"', q_start + 1);
			if (q_start != string::npos && q_end != string::npos) {
				result.access_type = json.substr(q_start + 1, q_end - q_start - 1);
			}
		}

		result.valid = true;
	} catch (const std::exception &) {
	}
	return result;
}

bool MySQLStatisticsCollector::HasUsableIndex(const string &schema, const string &table,
                                              const vector<string> &columns) {
	auto stats = GetTableStats(schema, table);
	return stats.FindBestIndex(columns) != nullptr;
}

double MySQLStatisticsCollector::GetDefaultSelectivity(ExpressionType comparison) const {
	switch (comparison) {
	case ExpressionType::COMPARE_EQUAL:
		return 0.01;
	case ExpressionType::COMPARE_NOTEQUAL:
		return 0.99;
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
		return 0.33;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return 0.33;
	default:
		return 0.10;
	}
}

double MySQLStatisticsCollector::EstimateSelectivityFromCardinality(idx_t cardinality, idx_t total_rows) const {
	if (cardinality == 0 || cardinality == DConstants::INVALID_INDEX || total_rows == 0) {
		return 0.10;
	}
	return 1.0 / static_cast<double>(cardinality);
}

double MySQLStatisticsCollector::EstimateSelectivity(const string &schema, const string &table, const string &column,
                                                     ExpressionType comparison, const Value &value) {
	auto stats = GetTableStats(schema, table);

	auto histogram = stats.GetHistogram(column);
	if (histogram) {
		double hist_selectivity = histogram->EstimateSelectivity(comparison, value, stats.estimated_row_count);
		if (hist_selectivity >= 0.0) {
			return hist_selectivity;
		}
	}

	auto it = stats.column_distinct_count.find(column);
	if (it != stats.column_distinct_count.end() && it->second > 0) {
		double base_selectivity = EstimateSelectivityFromCardinality(it->second, stats.estimated_row_count);
		switch (comparison) {
		case ExpressionType::COMPARE_EQUAL:
			return base_selectivity;
		case ExpressionType::COMPARE_NOTEQUAL:
			return 1.0 - base_selectivity;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return 0.33;
		default:
			return GetDefaultSelectivity(comparison);
		}
	}

	return GetDefaultSelectivity(comparison);
}

idx_t MySQLStatisticsCollector::EstimateFilteredRows(const string &schema, const string &table,
                                                     optional_ptr<TableFilterSet> filters) {
	auto stats = GetTableStats(schema, table);
	if (stats.estimated_row_count == 0) {
		return 0;
	}

	if (!filters || filters->filters.empty()) {
		return stats.estimated_row_count;
	}

	double combined_selectivity = 1.0;
	for (const auto &entry : filters->filters) {
		auto &filter = *entry.second;
		double filter_selectivity = 0.10;

		switch (filter.filter_type) {
		case TableFilterType::IS_NULL:
		case TableFilterType::IS_NOT_NULL:
			filter_selectivity = 0.01;
			break;
		case TableFilterType::CONSTANT_COMPARISON: {
			auto &constant_filter = filter.Cast<ConstantFilter>();
			filter_selectivity = GetDefaultSelectivity(constant_filter.comparison_type);
			break;
		}
		case TableFilterType::IN_FILTER: {
			auto &in_filter = filter.Cast<InFilter>();
			double per_value_selectivity = 0.01;
			if (in_filter.values.size() > 10) {
				per_value_selectivity = 0.005;
			}
			filter_selectivity = std::min(static_cast<double>(in_filter.values.size()) * per_value_selectivity, 0.8);
			break;
		}
		case TableFilterType::CONJUNCTION_AND: {
			auto &conj = filter.Cast<ConjunctionAndFilter>();
			double conj_selectivity = 1.0;
			for (idx_t i = 0; i < conj.child_filters.size(); i++) {
				conj_selectivity *= 0.10;
			}
			filter_selectivity = conj_selectivity;
			break;
		}
		case TableFilterType::CONJUNCTION_OR: {
			auto &conj = filter.Cast<ConjunctionOrFilter>();
			double non_selectivity = 1.0;
			for (idx_t i = 0; i < conj.child_filters.size(); i++) {
				non_selectivity *= (1.0 - 0.10);
			}
			filter_selectivity = 1.0 - non_selectivity;
			break;
		}
		case TableFilterType::OPTIONAL_FILTER:
			filter_selectivity = 0.10;
			break;
		default:
			filter_selectivity = 0.10;
			break;
		}

		combined_selectivity *= filter_selectivity;
	}

	idx_t estimated = static_cast<idx_t>(stats.estimated_row_count * combined_selectivity);
	return std::max(estimated, static_cast<idx_t>(1));
}

} // namespace duckdb
