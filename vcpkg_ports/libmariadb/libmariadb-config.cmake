include(CMakeFindDependencyMacro)

# Try vcpkg first (used in CI and standard builds)
find_package(unofficial-libmariadb CONFIG QUIET)
if(unofficial-libmariadb_FOUND)
    set(libmariadb_FOUND 1)
    set(MYSQL_LIBRARIES unofficial::libmariadb)
    return()
endif()

# Fall back to Homebrew mariadb-connector-c for local macOS development
# Check both Apple Silicon (/opt/homebrew) and Intel Mac (/usr/local) paths
foreach(HOMEBREW_PREFIX "/opt/homebrew" "/usr/local")
    set(HOMEBREW_MARIADB "${HOMEBREW_PREFIX}/opt/mariadb-connector-c")
    if(EXISTS "${HOMEBREW_MARIADB}")
        set(libmariadb_FOUND 1)
        set(MYSQL_INCLUDE_DIR "${HOMEBREW_MARIADB}/include/mariadb" CACHE PATH "MariaDB include directory")
        find_library(MYSQL_LIBRARY_PATH NAMES mariadb PATHS "${HOMEBREW_MARIADB}/lib" NO_DEFAULT_PATH)
        if(MYSQL_LIBRARY_PATH)
            set(MYSQL_LIBRARIES ${MYSQL_LIBRARY_PATH})
            message(STATUS "Found Homebrew mariadb-connector-c at ${HOMEBREW_MARIADB}")
            return()
        endif()
    endif()
endforeach()

message(FATAL_ERROR "Could not find libmariadb. Install via:\n  - vcpkg (for CI/production builds)\n  - brew install mariadb-connector-c (for local macOS development)")
