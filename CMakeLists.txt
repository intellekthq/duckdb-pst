cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME pst)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

# Export compile_commands.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(microsoft-pst-sdk)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wno-error=incompatible-ms-struct")

set(
  EXTENSION_SOURCES
  src/function_state.cpp
  src/table_function.cpp
  src/pst_extension.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Find deps
find_package(Boost 1.42.0 REQUIRED)
find_package(Iconv REQUIRED)

if(Boost_FOUND)
  list(APPEND DUCKPST_INCLUDE ${Boost_INCLUDE_DIRS})
  list(APPEND DUCKPST_LINK ${Boost_LIBRARIES})
endif()

if(Iconv_FOUND)
  list(APPEND DUCKPST_LINK Iconv::Iconv)
endif()

include_directories(${DUCKPST_INCLUDE})
target_link_libraries(${EXTENSION_NAME} ${DUCKPST_LINK})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
