deps <- gtools::getDependencies("epitweetr")
licences <- lapply(deps, function(p) list(package = p, license = packageDescription(p, fields="License")))
jsonlite::write_json(licences, "licenses/r-dep-licenses.json", pretty = T)

