
testDisk <- function(){
count = 0
size = 0
t <- tempfile()

start.time <- Sys.time()
for(i in 1:10000) {
  m <- matrix(rexp(20000, rate=.1), ncol=200)
  saveRDS(m, t)
  count = count + 1
  m <- readRDS(t)
  size = size + file.size(t) * 2
  if(i %% 100 == 0) {
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    secs = as.numeric(time.taken, units="secs")
    message(count, " files copied. Speed: ", size/(1024*1024*secs), " M/s")
  }
}
unlink(t)
}

testCpu <- function() {
no_cores <- parallel::detectCores() - 1
 
# Initiate cluster
cl <- parallel::makeCluster(no_cores)


start.time <- Sys.time()
count <- 0
for(i in 1:1000) {
    parallel::parLapply(cl, 1:no_cores,function(z){
    m <- matrix(rexp(200*200, rate=.1), ncol=200)
    eigen(m)
    1
  })
  count = count + no_cores
  if(i %% 100 == 0) {
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    secs = as.numeric(time.taken, units="secs")
    message(count, " Matrices processing speed: ", count/secs, " Mat/s")
  }
}
parallel::stopCluster(cl)
}




