cmake_minimum_required(VERSION 3.10)

set(PATCH_VERSION "0" CACHE INTERNAL "Patch version")
set(PROJECT_VESRION 0.0.${PATCH_VERSION})

project(mapreduce VERSION ${PROJECT_VESRION})

# include(FetchContent)
# FetchContent_Declare(
#   googletest
#   URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
# )
# For Windows: Prevent overriding the parent project's compiler/linker settings
# set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# FetchContent_MakeAvailable(googletest)

# configure_file(config.h.in config.h)

add_executable(mapreduce mapreduce.cpp mr_framework.cpp )
# add_library(main_control_lib main_control_lib.cpp)
# add_executable(test_main_control test_main_control.cpp)

set_target_properties(mapreduce  PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
)
target_include_directories(mapreduce
    PRIVATE "${CMAKE_BINARY_DIR}"
)

# target_link_libraries(main_control PRIVATE main_control_lib)
# target_link_libraries(test_main_control
#     GTest::gtest_main_control
#     main_control_lib
# )

if (MSVC)
    target_compile_options(mapreduce PRIVATE
        /W4
    )
    #  target_compile_options(test_main_control PRIVATE
    #     /W4
    # )
else ()
    target_compile_options(mapreduce PRIVATE
        -Wall -Wextra -pedantic -Werror
    )
    
endif()



install(TARGETS mapreduce RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

set(CPACK_PACKAGE_CONTACT alex-guerchoig@yandex.ru)

include(CPack)

# enable_testing()
# include(GoogleTest)
# gtest_discover_tests(test_main_control)
# add_test(test_main_control  test_main_control)



