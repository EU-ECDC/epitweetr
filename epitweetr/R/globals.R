# This global variable is to define .data global variable which is used on dplyr operations
# This avoids not defined variable warnings
utils::globalVariables(c(".data"))
