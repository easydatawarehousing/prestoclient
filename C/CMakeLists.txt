cmake_minimum_required (VERSION 2.8)

project(prestoclient)
set(TARGET_NAME "cprestoclient")

set(MY_CURL_DIR, /usr/lib/x86_64-linux-gnu)
find_library(MYCURL NAMES curl HINTS ${MY_CURL_DIR})

include_directories(./prestoclient)
include_directories(./prestoclient/curl)

file(GLOB EasyPTOra_SOURCES    src/*.c)
file(GLOB prestoclient_SOURCES prestoclient/*.c)
set(ALL_SOURCES ${EasyPTOra_SOURCES} ${prestoclient_SOURCES})

add_executable (${TARGET_NAME} ${ALL_SOURCES})

target_link_libraries(${TARGET_NAME} ${MYCURL})
