#include "duckdb.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "mysql_scanner.hpp"
#include "mysql_result.hpp"
#include "storage/mysql_transaction.hpp"
#include "storage/mysql_table_set.hpp"
#include "storage/mysql_catalog.hpp"
#include "storage/mysql_statistics.hpp"
#include "storage/mysql_predicate_analyzer.hpp"
#include "storage/federation/cost_model.hpp"
#include "mysql_filter_pushdown.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct MySQLGlobalState;

struct MySQLLocalState : public LocalTableFunctionState {};

struct MySQLGlobalState : public GlobalTableFunctionState {
	explicit MySQLGlobalState(unique_ptr<MySQLResult> result_p) : result(std::move(result_p)) {
	}

	unique_ptr<MySQLResult> result;

	idx_t total_rows_fetched = 0;
	ExecutionPlanCacheKey cache_key;
	idx_t estimated_rows = 0;
	idx_t cache_generation = 0;
	bool adaptive_feedback_enabled = false;
	double adaptive_replan_threshold = 3.0;
	idx_t adaptive_cooldown_seconds = 60;
	optional_ptr<MySQLCatalog> catalog;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MySQLBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	throw InternalException("MySQLBind");
}

static unique_ptr<GlobalTableFunctionState> MySQLInitGlobalState(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLBindData>();
	auto &transaction = MySQLTransaction::Get(context, bind_data.table.catalog);
	auto &con = transaction.GetConnection();

	string select;
	select += "SELECT ";
	for (idx_t c = 0; c < input.column_ids.size(); c++) {
		if (c > 0) {
			select += ", ";
		}
		if (input.column_ids[c] == COLUMN_IDENTIFIER_ROW_ID) {
			select += "NULL";
		} else {
			auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
			auto col_name = col.GetName();
			select += MySQLUtils::WriteIdentifier(col_name);
		}
	}
	select += " FROM ";
	select += MySQLUtils::WriteIdentifier(bind_data.table.schema.name);
	select += ".";
	select += MySQLUtils::WriteIdentifier(bind_data.table.name);

	string filter_string;
	if (bind_data.use_predicate_analyzer && input.filters && !input.filters->filters.empty()) {
		MySQLStatisticsCollector stats_collector(con);
		PredicateAnalyzer analyzer(stats_collector, bind_data.table.schema.name, bind_data.table.name);

		Value hint_enabled_val, hint_threshold_val;
		bool hint_injection_enabled = false;
		double hint_staleness_threshold = 0.5;
		if (context.TryGetCurrentSetting("mysql_hint_injection_enabled", hint_enabled_val)) {
			hint_injection_enabled = BooleanValue::Get(hint_enabled_val);
		}
		if (context.TryGetCurrentSetting("mysql_hint_staleness_threshold", hint_threshold_val)) {
			hint_staleness_threshold = hint_threshold_val.GetValue<double>();
		}
		analyzer.SetHintInjectionEnabled(hint_injection_enabled, hint_staleness_threshold);

		bind_data.filter_analysis = analyzer.AnalyzeFilters(input.column_ids, input.filters, bind_data.names);

		vector<string> column_names;
		vector<string> filter_columns;
		vector<double> selectivities;
		for (idx_t c = 0; c < input.column_ids.size(); c++) {
			if (input.column_ids[c] != COLUMN_IDENTIFIER_ROW_ID) {
				auto &col = bind_data.table.GetColumn(LogicalIndex(input.column_ids[c]));
				column_names.push_back(col.GetName());
			}
		}
		for (const auto &analysis : bind_data.filter_analysis.analyses) {
			filter_columns.push_back(analysis.column_name);
			selectivities.push_back(analysis.estimated_selectivity);
		}

		auto &mysql_catalog = bind_data.table.catalog.Cast<MySQLCatalog>();

		ExecutionPlanCacheKey cache_key;
		cache_key.schema_name = bind_data.table.schema.name;
		cache_key.table_name = bind_data.table.name;
		cache_key.column_names = column_names;
		cache_key.filter_columns = filter_columns;
		cache_key.selectivities = selectivities;

		idx_t cache_generation = 0;
		idx_t estimated_rows = 0;

		bool cache_hit = false;
		auto cached_plan = mysql_catalog.GetCachedPlan(cache_key, cache_hit);
		if (cache_hit) {
			bind_data.execution_plan.strategy = cached_plan.strategy;
			bind_data.execution_plan.pushed_filter_indices = cached_plan.pushed_filter_indices;
			bind_data.execution_plan.local_filter_indices = cached_plan.local_filter_indices;
			cache_generation = cached_plan.generation;
			estimated_rows = cached_plan.estimated_rows;
		} else {
			auto table_stats = stats_collector.GetTableStats(bind_data.table.schema.name, bind_data.table.name);
			auto mysql_costs = stats_collector.FetchMySQLCostConstants();
			CostModelParameters cost_params;
			if (mysql_costs.loaded) {
				cost_params.cpu_cost_per_row = mysql_costs.row_evaluate_cost;
				cost_params.io_cost_per_byte = mysql_costs.io_block_read_cost / 16384.0;
			}
			DefaultCostModel cost_model(cost_params);
			mysql_catalog.GetConnectionPool().EnsureCalibrated(con);
			cost_model.SetNetworkCalibration(mysql_catalog.GetConnectionPool().GetNetworkCalibration());
			bind_data.execution_plan = cost_model.ComparePlans(table_stats, bind_data.filter_analysis, column_names);

			estimated_rows = bind_data.execution_plan.estimated_rows_from_mysql;
			mysql_catalog.CachePlan(cache_key, bind_data.execution_plan.strategy, estimated_rows,
			                        bind_data.execution_plan.pushed_filter_indices,
			                        bind_data.execution_plan.local_filter_indices);
		}

		bind_data.adaptive_cache_key = cache_key;
		bind_data.adaptive_cache_generation = cache_generation;
		bind_data.adaptive_estimated_rows = estimated_rows;

		{
			auto part_stats = stats_collector.GetTableStats(bind_data.table.schema.name, bind_data.table.name);
			const auto &part_info = part_stats.partition_info;
			if (part_info.IsRangeOrList() && !part_info.partitions.empty()) {
				for (const auto &analysis : bind_data.filter_analysis.analyses) {
					if (!analysis.is_partition_key || !analysis.ShouldPush()) {
						continue;
					}
					for (const auto &entry : input.filters->filters) {
						column_t col_idx = entry.first;
						if (col_idx >= input.column_ids.size()) {
							continue;
						}
						column_t actual_col_idx = input.column_ids[col_idx];
						if (actual_col_idx >= bind_data.names.size()) {
							continue;
						}
						if (!StringUtil::CIEquals(bind_data.names[actual_col_idx], analysis.column_name)) {
							continue;
						}
						auto &filter = *entry.second;
						if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
							auto &cf = filter.Cast<ConstantFilter>();
							auto matching = part_info.GetMatchingPartitions(cf.constant, cf.comparison_type);
							if (!matching.empty() && matching.size() < part_info.partitions.size()) {
								vector<string> escaped;
								for (const auto &p : matching) {
									escaped.push_back(MySQLUtils::WriteIdentifier(p));
								}
								bind_data.partition_clause = "PARTITION (" + StringUtil::Join(escaped, ", ") + ")";
							}
						}
						break;
					}
					if (!bind_data.partition_clause.empty()) {
						break;
					}
				}
			}
		}

		switch (bind_data.execution_plan.strategy) {
		case ExecutionStrategy::PUSH_ALL_FILTERS:
			filter_string = bind_data.filter_analysis.combined_mysql_predicate;
			break;
		case ExecutionStrategy::EXECUTE_ALL_LOCALLY:
			throw InternalException("EXECUTE_ALL_LOCALLY strategy is not implemented");
			break;
		case ExecutionStrategy::HYBRID: {
			vector<string> pushed_predicates;
			for (const auto idx : bind_data.execution_plan.pushed_filter_indices) {
				if (idx < bind_data.filter_analysis.analyses.size()) {
					const auto &analysis = bind_data.filter_analysis.analyses[idx];
					if (!analysis.mysql_predicate.empty()) {
						pushed_predicates.push_back(analysis.mysql_predicate);
					}
				}
			}
			filter_string = StringUtil::Join(pushed_predicates, " AND ");
			break;
		}
		}
	} else {
		filter_string = MySQLFilterPushdown::TransformFilters(input.column_ids, input.filters, bind_data.names);
	}

	if (bind_data.use_predicate_analyzer && !bind_data.partition_clause.empty()) {
		select += " " + bind_data.partition_clause;
	}

	if (bind_data.use_predicate_analyzer && !bind_data.filter_analysis.recommended_index.empty()) {
		string index_identifier = MySQLUtils::WriteIdentifier(bind_data.filter_analysis.recommended_index);
		if (bind_data.filter_analysis.suggest_force_index) {
			select += " FORCE INDEX (" + index_identifier + ")";
		} else {
			select += " USE INDEX (" + index_identifier + ")";
		}
	}

	if (!filter_string.empty()) {
		select += " WHERE " + filter_string;
	}
	if (!bind_data.limit.empty()) {
		select += bind_data.limit;
	}

	if (bind_data.use_predicate_analyzer) {
		Value explain_val;
		if (context.TryGetCurrentSetting("mysql_explain_validation_enabled", explain_val) &&
		    BooleanValue::Get(explain_val) && !bind_data.filter_analysis.recommended_index.empty()) {
			MySQLStatisticsCollector explain_stats(con);
			bool uses_index = false;
			for (const auto &analysis : bind_data.filter_analysis.analyses) {
				if (analysis.has_index && analysis.ShouldPush()) {
					uses_index = true;
					break;
				}
			}
			if (uses_index) {
				bool valid =
				    explain_stats.ValidateWithExplain(select, bind_data.filter_analysis.recommended_index, true);
				if (!valid) {
					auto &mysql_catalog = bind_data.table.catalog.Cast<MySQLCatalog>();
					mysql_catalog.ClearCache();
				}
			}
		}

		Value timeout_val;
		bool timeout_enabled = true;
		if (context.TryGetCurrentSetting("mysql_query_timeout_enabled", timeout_val)) {
			timeout_enabled = BooleanValue::Get(timeout_val);
		}
		if (timeout_enabled && bind_data.execution_plan.estimated_cost.Total() > 0) {
			idx_t timeout_ms = std::max(static_cast<idx_t>(5000),
			                            static_cast<idx_t>(bind_data.execution_plan.estimated_cost.Total() * 10));
			string hint = "/*+ MAX_EXECUTION_TIME(" + std::to_string(timeout_ms) + ") */";
			size_t after_select = select.find("SELECT ") + 7;
			select.insert(after_select, hint + " ");
		}

		Value buffer_val;
		bool buffer_enabled = true;
		if (context.TryGetCurrentSetting("mysql_sql_buffer_result", buffer_val)) {
			buffer_enabled = BooleanValue::Get(buffer_val);
		}
		if (buffer_enabled && bind_data.adaptive_estimated_rows > 10000) {
			size_t after_select = select.find("SELECT ") + 7;
			if (select.find("SQL_BUFFER_RESULT") == string::npos) {
				select.insert(after_select, "SQL_BUFFER_RESULT ");
			}
		}
	}

	auto query_result = con.Query(select, bind_data.streaming);
	auto result = make_uniq<MySQLGlobalState>(std::move(query_result));

	if (bind_data.use_predicate_analyzer) {
		result->cache_key = bind_data.adaptive_cache_key;
		result->cache_generation = bind_data.adaptive_cache_generation;
		result->estimated_rows = bind_data.adaptive_estimated_rows;
		result->catalog = &bind_data.table.catalog.Cast<MySQLCatalog>();

		Value adaptive_enabled_val, replan_threshold_val, cooldown_val;
		if (context.TryGetCurrentSetting("mysql_adaptive_replan_enabled", adaptive_enabled_val)) {
			result->adaptive_feedback_enabled = BooleanValue::Get(adaptive_enabled_val);
		}
		if (context.TryGetCurrentSetting("mysql_adaptive_replan_threshold", replan_threshold_val)) {
			result->adaptive_replan_threshold = replan_threshold_val.GetValue<double>();
		}
		if (context.TryGetCurrentSetting("mysql_adaptive_cooldown_seconds", cooldown_val)) {
			result->adaptive_cooldown_seconds = cooldown_val.GetValue<uint64_t>();
		}
	}

	return result;
}

static unique_ptr<LocalTableFunctionState> MySQLInitLocalState(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<MySQLLocalState>();
}

static void MySQLScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (gstate.result->Exhausted()) {
		if (gstate.adaptive_feedback_enabled && gstate.catalog && gstate.estimated_rows > 0) {
			gstate.catalog->UpdatePlanFeedback(gstate.cache_key, gstate.estimated_rows, gstate.total_rows_fetched,
			                                   gstate.cache_generation, gstate.adaptive_replan_threshold,
			                                   gstate.adaptive_cooldown_seconds);
		}
		output.SetCardinality(0);
		return;
	}

	DataChunk &res_chunk = gstate.result->NextChunk();
	gstate.total_rows_fetched += res_chunk.size();
	D_ASSERT(output.ColumnCount() == res_chunk.ColumnCount());
	string error;
	for (idx_t c = 0; c < output.ColumnCount(); c++) {
		Vector &output_vec = output.data[c];
		Vector &res_vec = res_chunk.data[c];
		switch (output_vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BLOB:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP: {
			if (output_vec.GetType().id() == res_vec.GetType().id() ||
			    (output_vec.GetType().id() == LogicalTypeId::BLOB &&
			     res_vec.GetType().id() == LogicalTypeId::VARCHAR)) {
				output_vec.Reinterpret(res_vec);
			} else {
				VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
			}
			break;
		}
		default: {
			VectorOperations::TryCast(context, res_vec, output_vec, res_chunk.size(), &error);
			break;
		}
		}
		if (!error.empty()) {
			throw BinderException(error);
		}
	}
	output.SetCardinality(res_chunk.size());
}

static InsertionOrderPreservingMap<string> MySQLScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<MySQLBindData>();
	result["Table"] = bind_data.table.name;
	return result;
}

static void MySQLScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	throw NotImplementedException("MySQLScanSerialize");
}

static unique_ptr<FunctionData> MySQLScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("MySQLScanDeserialize");
}

static BindInfo MySQLGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MySQLBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table;
	return info;
}

MySQLScanFunction::MySQLScanFunction()
    : TableFunction("mysql_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLScan,
                    MySQLBind, MySQLInitGlobalState, MySQLInitLocalState) {
	to_string = MySQLScanToString;
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	get_bind_info = MySQLGetBindInfo;
	projection_pushdown = true;
}

//===--------------------------------------------------------------------===//
// MySQL Query
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> MySQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("Parameters to mysql_query cannot be NULL");
	}

	// look up the database to query
	auto db_name = input.inputs[0].GetValue<string>();
	auto &db_manager = DatabaseManager::Get(context);
	auto db = db_manager.GetDatabase(context, db_name);
	if (!db) {
		throw BinderException("Failed to find attached database \"%s\" referenced in mysql_query", db_name);
	}
	auto &catalog = db->GetCatalog();
	if (catalog.GetCatalogType() != "mysql") {
		throw BinderException("Attached database \"%s\" does not refer to a MySQL database", db_name);
	}
	auto &transaction = MySQLTransaction::Get(context, catalog);
	auto sql = input.inputs[1].GetValue<string>();

	vector<Value> params;
	auto params_it = input.named_parameters.find("params");
	if (params_it != input.named_parameters.end()) {
		Value &struct_val = params_it->second;
		if (struct_val.IsNull()) {
			throw BinderException("Parameters to mysql_query cannot be NULL");
		}
		if (struct_val.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("Query parameters must be specified in a STRUCT");
		}
		params = StructValue::GetChildren(struct_val);
	}

	MySQLResultStreaming streaming = MySQLResultStreaming::ALLOW_STREAMING;
	auto streaming_it = input.named_parameters.find("stream_results");
	if (streaming_it != input.named_parameters.end()) {
		Value &bool_val = streaming_it->second;
		if (!bool_val.IsNull()) {
			if (BooleanValue::Get(bool_val)) {
				streaming = MySQLResultStreaming::REQUIRE_STREAMING;
			} else {
				streaming = MySQLResultStreaming::FORCE_MATERIALIZATION;
			}
		}
	}

	MySQLConnection &conn = transaction.GetConnection();

	if (streaming == MySQLResultStreaming::FORCE_MATERIALIZATION) {
		auto result = conn.Query(sql, params, MySQLResultStreaming::FORCE_MATERIALIZATION);
		for (auto &field : result->Fields()) {
			names.push_back(field.name);
			return_types.push_back(field.duckdb_type);
		}
		return make_uniq<MySQLQueryBindData>(catalog, std::move(result), std::move(sql), streaming);
	}

	auto stmt = transaction.GetConnection().Prepare(sql);
	for (auto &field : stmt->Fields()) {
		names.push_back(field.name);
		return_types.push_back(field.duckdb_type);
	}
	return make_uniq<MySQLQueryBindData>(catalog, std::move(stmt), std::move(params), std::move(sql), streaming);
}

static unique_ptr<GlobalTableFunctionState> MySQLQueryInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<MySQLQueryBindData>();
	unique_ptr<MySQLResult> mysql_result;
	if (bind_data.result) {
		mysql_result = std::move(bind_data.result);
	}
	return make_uniq<MySQLGlobalState>(std::move(mysql_result));
}

static void MySQLQueryScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<MySQLGlobalState>();
	if (!gstate.result) {
		auto &bind_data = data.bind_data->CastNoConst<MySQLQueryBindData>();
		if (bind_data.user_streaming == MySQLResultStreaming::REQUIRE_STREAMING &&
		    bind_data.streaming != MySQLResultStreaming::ALLOW_STREAMING) {
			throw IOException("Unable to stream results of 'mysql_query' function, ensure that each invocation with "
			                  "'stream_results=TRUE' uses its own private connection");
		}
		auto &transaction = MySQLTransaction::Get(context, bind_data.catalog);
		MySQLStatement &stmt = *bind_data.stmt;
		const vector<Value> &params = bind_data.params;
		auto result = transaction.GetConnection().Query(stmt, params, bind_data.streaming);
		gstate.result = std::move(result);
	}
	MySQLScan(context, data, output);
}

MySQLQueryFunction::MySQLQueryFunction()
    : TableFunction("mysql_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, MySQLQueryScan, MySQLQueryBind,
                    MySQLQueryInitGlobalState, MySQLInitLocalState) {
	serialize = MySQLScanSerialize;
	deserialize = MySQLScanDeserialize;
	named_parameters["params"] = LogicalType::ANY;
	named_parameters["stream_results"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
