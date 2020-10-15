# this global variable is to define .data global variable which is used on dplyr operations
# this avoid not defined variable warnings
utils::globalVariables(c(".data"))
