cmake_minimum_required(VERSION 3.20)
project(history_cache VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
option(HISTORY_CACHE_SANITIZERS "Enable address and undefined behavior sanitizers" OFF)
option(HISTORY_CACHE_CURL "Build the explicitly gated HTTPS transport" OFF)
set(HISTORY_CACHE_SANITIZER_SET "address,undefined" CACHE STRING "Sanitizers: address, undefined, or address,undefined")

find_package(OpenSSL REQUIRED COMPONENTS Crypto)
find_package(PkgConfig REQUIRED)
pkg_check_modules(ZSTD REQUIRED IMPORTED_TARGET libzstd>=1.4.0)
find_package(nlohmann_json 3.10 REQUIRED)

add_library(history_cache_core
    src/core/common.cpp
    src/core/columns.cpp
    src/core/pack.cpp
    src/core/catalog.cpp
    src/core/object_store.cpp
    src/transport/http.cpp
    src/transport/sigv4.cpp
    src/transport/s3_store.cpp
    src/transport/curl_transport.cpp
    src/publisher/local_store.cpp
    src/publisher/publisher.cpp
    src/publisher/factor_publisher.cpp
    src/publisher/journal.cpp
    src/staging/plan.cpp
    src/staging/ledger.cpp
    src/staging/executor.cpp
    src/agent/reader.cpp
    src/agent/adjustment.cpp
    src/agent/factor_snapshot.cpp
    src/agent/market_clock.cpp
    src/agent/maintenance.cpp
    src/agent/service.cpp)
target_include_directories(history_cache_core PUBLIC include)
target_include_directories(history_cache_core PRIVATE src/core)
target_link_libraries(history_cache_core PUBLIC
    OpenSSL::Crypto PkgConfig::ZSTD nlohmann_json::nlohmann_json)
target_compile_options(history_cache_core PRIVATE -Wall -Wextra -Wconversion -Wshadow)
# GCC 11 (Rocky 9) has false-positive optimizer warnings; enforce -Werror only on GCC 12+.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 12)
    target_compile_options(history_cache_core PRIVATE -Werror)
endif()

if(HISTORY_CACHE_CURL)
    # 7.76 covers Rocky 9 (7.76), matching the cloud_gateway_v2 base image.
    # The runtime transport also checks that the loaded libcurl version
    # matches the compile-time version exactly, which is guaranteed by
    # the multi-stage container build.
    find_package(CURL 7.76 REQUIRED)
    target_link_libraries(history_cache_core PRIVATE CURL::libcurl)
    target_compile_definitions(history_cache_core PUBLIC HC_HAS_CURL=1 HC_CURL_VERSION="${CURL_VERSION_STRING}")
endif()

if(HISTORY_CACHE_SANITIZERS)
    if(NOT HISTORY_CACHE_SANITIZER_SET MATCHES "^(address|undefined|address,undefined)$")
        message(FATAL_ERROR "Unsupported HISTORY_CACHE_SANITIZER_SET")
    endif()
    target_compile_options(history_cache_core PUBLIC -fsanitize=${HISTORY_CACHE_SANITIZER_SET} -fno-omit-frame-pointer)
    target_link_options(history_cache_core PUBLIC -fsanitize=${HISTORY_CACHE_SANITIZER_SET})
endif()

add_executable(history-cache-tool tools/history_cache_tool.cpp)
target_link_libraries(history-cache-tool PRIVATE history_cache_core)
target_compile_options(history-cache-tool PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)

add_executable(history-cache-sample tools/history_cache_sample.cpp)
target_link_libraries(history-cache-sample PRIVATE history_cache_core)
target_compile_options(history-cache-sample PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)

add_executable(history-cache-snapshot tools/history_cache_snapshot.cpp)
target_link_libraries(history-cache-snapshot PRIVATE history_cache_core)
target_compile_options(history-cache-snapshot PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)

add_executable(history-cache-agent tools/history_cache_agent.cpp)
add_executable(history-cache-factors tools/history_cache_factors.cpp)
target_link_libraries(history-cache-factors PRIVATE history_cache_core)
target_compile_options(history-cache-factors PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
target_link_libraries(history-cache-agent PRIVATE history_cache_core pthread)
target_compile_options(history-cache-agent PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)

add_executable(history-cache-staging tools/history_cache_staging.cpp)
target_link_libraries(history-cache-staging PRIVATE history_cache_core)
set(staging_workspace "${CMAKE_CURRENT_SOURCE_DIR}")
get_filename_component(component_parent "${CMAKE_CURRENT_SOURCE_DIR}" DIRECTORY)
get_filename_component(component_parent_name "${component_parent}" NAME)
if(component_parent_name STREQUAL "components" AND EXISTS "${component_parent}/../release-manifest.yaml")
    get_filename_component(staging_workspace "${component_parent}/.." REALPATH)
endif()
target_compile_definitions(history-cache-staging PRIVATE HC_STAGING_WORKSPACE="${staging_workspace}")
target_compile_options(history-cache-staging PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    add_test(NAME factor_cli COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_factors_cli.py
        --tool $<TARGET_FILE:history-cache-factors>)
    set_tests_properties(factor_cli PROPERTIES TIMEOUT 30)
    add_executable(history-cache-adjustment-tests tests/adjustment_tests.cpp)
    target_link_libraries(history-cache-adjustment-tests PRIVATE history_cache_core)
    target_compile_options(history-cache-adjustment-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    add_test(NAME adjustment COMMAND history-cache-adjustment-tests)
    set_tests_properties(adjustment PROPERTIES TIMEOUT 15)
    add_executable(history-cache-agent-tests tests/agent_tests.cpp)
    target_link_libraries(history-cache-agent-tests PRIVATE history_cache_core pthread)
    target_compile_options(history-cache-agent-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    add_test(NAME agent_service COMMAND history-cache-agent-tests)
    add_test(NAME agent_lifetime COMMAND history-cache-agent-tests lifetime)
    add_test(NAME agent_uds COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_agent_uds.py
        --agent $<TARGET_FILE:history-cache-agent>)
    set_tests_properties(agent_service agent_uds PROPERTIES TIMEOUT 30)
    set_tests_properties(agent_lifetime PROPERTIES TIMEOUT 80)
    add_test(NAME snapshot_cli COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_snapshot.py --tool $<TARGET_FILE:history-cache-snapshot>)
    set_tests_properties(snapshot_cli PROPERTIES TIMEOUT 90 ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1")
    add_executable(history-cache-sample-transfer-tests tests/sample_transfer_tests.cpp)
    target_link_libraries(history-cache-sample-transfer-tests PRIVATE history_cache_core)
    target_compile_options(history-cache-sample-transfer-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    add_test(NAME sample_transfer COMMAND history-cache-sample-transfer-tests)
    set_tests_properties(sample_transfer PROPERTIES TIMEOUT 15)
    add_test(NAME sample_cli COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_sample.py --tool $<TARGET_FILE:history-cache-sample>)
    set_tests_properties(sample_cli PROPERTIES TIMEOUT 30 ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1")
    add_executable(history-cache-staging-tests tests/staging_tests.cpp)
    target_link_libraries(history-cache-staging-tests PRIVATE history_cache_core)
    target_include_directories(history-cache-staging-tests PRIVATE src/core)
    target_compile_definitions(history-cache-staging-tests PRIVATE HC_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    target_compile_options(history-cache-staging-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/tests/staging-cases.json" staging_cases)
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS tests/staging-cases.json)
    string(JSON suite_count LENGTH "${staging_cases}")
    if(NOT suite_count EQUAL 5)
        message(FATAL_ERROR "Incomplete staging case matrix")
    endif()
    set(staging_plan_count 5)
    set(staging_execution_count 13)
    set(staging_ledger_count 9)
    set(staging_delete_count 3)
    set(staging_retention_count 20)
    foreach(suite staging_plan staging_execution staging_ledger staging_delete staging_retention)
        string(JSON count LENGTH "${staging_cases}" ${suite})
        if(NOT count EQUAL ${suite}_count)
            message(FATAL_ERROR "Incomplete ${suite} case matrix")
        endif()
        math(EXPR last "${count} - 1")
        foreach(index RANGE ${last})
            string(JSON case_name GET "${staging_cases}" ${suite} ${index})
            add_test(NAME ${suite}.${case_name} COMMAND history-cache-staging-tests ${suite} ${case_name})
            set_tests_properties(${suite}.${case_name} PROPERTIES TIMEOUT 30)
        endforeach()
    endforeach()
    add_test(NAME staging_cli COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_staging_cli.py
        --tool $<TARGET_FILE:history-cache-tool> --staging $<TARGET_FILE:history-cache-staging>
        --workspace ${staging_workspace})
    set_tests_properties(staging_cli PROPERTIES TIMEOUT 30 ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1")
    add_executable(history-cache-tests tests/core_tests.cpp)
    target_link_libraries(history-cache-tests PRIVATE history_cache_core)
    target_include_directories(history-cache-tests PRIVATE src/core)
    target_compile_definitions(history-cache-tests PRIVATE HC_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    target_compile_options(history-cache-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    foreach(suite columns pack catalog store reader)
        add_test(NAME ${suite} COMMAND history-cache-tests ${suite})
        set_tests_properties(${suite} PROPERTIES TIMEOUT 15)
    endforeach()
    add_executable(history-cache-object-tests tests/object_store_tests.cpp)
    target_link_libraries(history-cache-object-tests PRIVATE history_cache_core)
    target_compile_options(history-cache-object-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    foreach(suite object_store conditional_publish object_reader)
        add_test(NAME ${suite} COMMAND history-cache-object-tests ${suite})
        set_tests_properties(${suite} PROPERTIES TIMEOUT 15)
    endforeach()
    add_executable(history-cache-transport-tests tests/transport_tests.cpp)
    target_link_libraries(history-cache-transport-tests PRIVATE history_cache_core)
    target_compile_definitions(history-cache-transport-tests PRIVATE HC_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
    target_compile_options(history-cache-transport-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    foreach(suite http_protocol sigv4 s3_protocol curl_offline)
        add_test(NAME ${suite} COMMAND history-cache-transport-tests ${suite})
        set_tests_properties(${suite} PROPERTIES TIMEOUT 15)
    endforeach()
    add_test(NAME sigv4_reference COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/check_sigv4.py)
    set_tests_properties(sigv4_reference PROPERTIES TIMEOUT 20 ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1")
    add_executable(history-cache-journal-tests tests/journal_tests.cpp)
    target_link_libraries(history-cache-journal-tests PRIVATE history_cache_core)
    target_compile_options(history-cache-journal-tests PRIVATE -Wall -Wextra -Wconversion -Wshadow -Werror)
    foreach(suite journal_storage journal_recovery)
        add_test(NAME ${suite} COMMAND history-cache-journal-tests ${suite})
        set_tests_properties(${suite} PROPERTIES TIMEOUT 15)
    endforeach()
    add_test(NAME offline_interop COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_offline.py
        --tool $<TARGET_FILE:history-cache-tool> --small-only)
    add_test(NAME offline_streaming_500k COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_offline.py
        --tool $<TARGET_FILE:history-cache-tool> --large-only)
    set_tests_properties(offline_interop offline_streaming_500k PROPERTIES TIMEOUT 20
        ENVIRONMENT "PYTHONDONTWRITEBYTECODE=1")
endif()
