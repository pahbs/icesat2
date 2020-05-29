#! /bin/bash
#
# This will filter a ICESat-2 ATL08 CSV
#
#

suppressMessages(library(rgdal))
suppressMessages(library(raster))
suppressMessages(library(stringr))
suppressMessages(library(sf))
suppressMessages(library(fs))
suppressMessages(library(tidyverse))
suppressMessages(library(optparse))
suppressMessages(library(tools))
suppressMessages(library(plyr))

option_list = list(
  make_option(c("--indir"),           type="character", default=NULL, help="Input ATL08 csv dir [default= %default]", metavar="character"),
  make_option(c("--outdir"),          type="character", default=NULL, help="Output dir of merged csv file [default= %default]"),
  make_option(c("--csv_search_str"),   type="character", default=NULL, help="Search string for input CSVs to merge [default= %default]")
);

main <- function(){
  
  #''' Merge list of ICESat-2 ATL08 CSVs
  #       return a merged CSV
  #'''

  opt_parser = OptionParser(option_list=option_list);
  opt = parse_args(opt_parser);
  
  if(is.null(opt$indir)){
    print_help(opt_parser)
    stop("Must provide an input dir", call.=FALSE)
  }
  if(is.null(opt$outdir)){
    outdir = dirname(opt$indir)
    print(paste0("Output dir will be 1 level up from input dir: ", outdir))
  }else{
    outdir = opt$outdir
    print(paste0("Output dir will be: ", outdir))
  }

  if(is.null(opt$csv_search_str)){
    print_help(opt_parser)
    stop("Must provide a search string", call.=FALSE)
  }

  # Get list of csv files to merge
  csv_list = list.files(path = opt$indir, pattern = paste0("\\",opt$csv_search_str,".csv$"), full.names=TRUE)
  
  atl08_merged_csv_fn = path(outdir, paste0("ATL08_merged_",opt$csv_search_str,".csv"))  

  df_all = do.call("rbind",lapply(csv_list, FUN=function(files){ read.csv(files)}))

  # Write out a massive CSV of all ATL08 points from H5 files youve found
  write.csv(df_all, file = atl08_merged_csv_fn, row.names=FALSE)
}

main()