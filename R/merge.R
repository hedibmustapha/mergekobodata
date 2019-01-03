

#' Merging data files from deviating kobo tools
#'
#' @param folder the path to the folder containing the files to be merged. The files must be in csv format.
#' @param filename_pattern a search term to find files that should be merged (inside the folder). You can use R style 'regex' patterns if you set use_regex=T
#' @param use_regex if TRUE, searches the 'folder' directory for 'filename_pattern' using regex patterns. Defaults to FALSE.
#' @param output_file full path to the file the merged dataset should be written to. If NULL (default), no output file is written.
#' @param add_file_reference If TRUE (default), adds a new column called `MERGED_FROM_FILE` containing the file name of each records source file.
#' @param write_log if TRUE, writes an html file with a log of the merge and subsequent checks.
#' @param verbose if TRUE writes messages to console during merge; set to FALSE to make quiet.
#' @return the merged data frame.
merge_kobo_data<-function(folder,filename_pattern='.',output_file=NULL,write_log=T,use_regex=F,add_file_reference=T,verbose=T){

  find_files<-function(folder,filename_pattern='.',use_regex=use_regex){
    filenames<- grep(filename_pattern,list.files(folder),fixed = !use_regex,value=T)
    filenames
  }

  # input sanitation:
  if(!is.character(folder)){stop('`folder` must be a single string')}
  if(length(folder)!=1){stop('`folder` only takes a single element')}

  if(!is.character(filename_pattern)){stop('`filename_pattern` must be a single string')}
  if(length(filename_pattern)!=1){stop('`filename_pattern` only takes a single element')}

  if(!is.character(output_file)){stop('`output_file` must be a single string')}
  if(length(output_file)!=1){stop('`output_file` only takes a single element')}

  if(!is.logical(write_log)){stop('`write_log` must be logical (TRUE or FALSE)')}
  if(length(write_log)!=1){stop('`write_log` only takes a single logical element, not multiple.')}

  if(!is.logical(use_regex)){stop('`use_regex` must be logical (TRUE or FALSE)')}
  if(length(use_regex)!=1){stop('`use_regex` only takes a single logical element, not multiple.')}

  if(!is.logical(add_file_reference)){stop('`add_file_reference` must be logical (TRUE or FALSE)')}
  if(length(add_file_reference)!=1){stop('`add_file_reference` only takes a single logical element, not multiple.')}

  if(!is.logical(verbose)){stop('`verbose` must be logical (TRUE or FALSE)')}
  if(length(verbose)!=1){stop('`verbose` only takes a single logical element, not multiple.')}

  if(!dir.exists(folder)){stop(paste0("`folder`:",folder,'not found. current working directory is:',getwd(),collapse='\\n'))}



  filenames<-find_files(folder,filename_pattern,use_regex)
  if(length(filenames)==0){stop('no files found that match the search pattern')}
  if(!all(grepl('.csv$',filenames))){stop('all files in the input folder that match the filename_pattern must have the extension ".csv"')}
  if(!all(grepl('.csv$',output_file))){stop('the output file name must have the extension ".csv"')}

  # load all datasets from csv
  filepaths<-paste0(folder,'/',filenames)
  if(verbose){message(paste0(c('merging files:',filepaths),collapse='\n'))}
  d<-lapply(filepaths,utils::read.csv,stringsAsFactors=F)
  if(add_file_reference){
  # add source file references
  add_source_file_reference<-function(fn,d){d$MERGED_FROM_FILE<-gsub('\\.csv','',fn);d}
  d<-mapply(add_source_file_reference,filenames,d,SIMPLIFY = F)
  }

  # bind datasets together and fill missing columns with NA:
  combined<-do.call((plyr::rbind.fill),d)
  # save combined dataset
  if(verbose){message(paste0('writing merged file to:\n',output_file))}
  utils::write.csv(combined,output_file)

  if(write_log){
    logfilepath<-paste0(gsub('\\.csv','',output_file),'_LOG.html')
    sink(logfilepath)
    cat('<h1>merge log</h1>')
    cat('<h3>info</h3>')
    cat('<h3>number of records</h3>')
    if(add_file_reference){
      countedrecords<-as.data.frame(table(combined$MERGED_FROM_FILE))
      colnames(countedrecords)<-(c('source','# records'))
      knitr::kable(countedrecords,format='html')
    }
      cat('<h3>total valid values pre merge</h3>')
      cat(unname(table(is.na(unlist(d)))['FALSE']))
    cat('<h3>total valid values post merge</h3>')
    cat(unname(table(is.na(unlist(combined)))['FALSE']))
    sink()
    if(verbose){message(paste0('log written to:',logfilepath))}
  }

  combined<-combined
}




