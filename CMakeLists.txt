cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME pst)

# NOTE: GCC link breaks due to ABI mismatch for static constexpr handling, but
# our CMakeLists.txt is loaded second. Must pass in env or Makefile too.
set(CMAKE_CXX_STANDARD "17")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# Export compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(MSVC)
  set(CMAKE_CXX_FLAGS "/DPSTSDK_VALIDATION_LEVEL_FULL /DVC_EXTRALEAN /DWIN32_LEAN_AND_MEAN /D_HAS_STD_BYTE=0 /EHsc /nologo /std:c++17 /W4 /WX /wd4244 /wd4267 /wd4456 /wd4127 /wd4100 /wd4458 /wd4146")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  # https://clang.llvm.org/docs/DiagnosticsReference.html#wincompatible-ms-struct
  set(CMAKE_CXX_FLAGS "-Wall -Wno-error=incompatible-ms-struct -g -DPSTSDK_VALIDATION_LEVEL_FULL -std=c++17")
elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  set(CMAKE_CXX_FLAGS "-Wall -g -DPSTSDK_VALIDATION_LEVEL_FULL -std=c++17")
endif()

include_directories(src/include)
include_directories(SYSTEM microsoft-pst-sdk)

set(
  EXTENSION_SOURCES
  src/filter_optimizer.cpp
  src/function_state.cpp
  src/table_function.cpp
  src/pst_extension.cpp
  src/row_serializer.cpp
  src/pst/duckdb_filesystem.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

find_package(Boost REQUIRED COMPONENTS thread)
if(Boost_FOUND)
  list(APPEND DUCKPST_INCLUDE ${Boost_INCLUDE_DIRS})
  list(APPEND DUCKPST_LINK Boost::thread)
endif()

# GNU iconv is required on macOS to avoid decoder bug with UTF-16LE.
# The custom vcpkg overlay port in extension-ci-tools/vcpkg_ports/libiconv
# forces GNU libiconv to build on macOS instead of using the system stub.
find_package(Iconv REQUIRED)
if(Iconv_FOUND)
  list(APPEND DUCKPST_LINK Iconv::Iconv)
endif()

include_directories(${DUCKPST_INCLUDE})
target_link_libraries(${EXTENSION_NAME} ${DUCKPST_LINK})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${DUCKPST_LINK})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
