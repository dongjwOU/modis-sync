This project scrapes USGS' MODIS http server at http://e4ftl01.cr.usgs.gov/MOLT/. It looks for specific data products, gets a list of HDF files, and filters by tile and date. The final file list is used to download data locally and then upload it to S3 - if and only if the file is not already on S3. Right now it's hardcoded to look for Terra products.

This project is used to update data for the [forma-clj](https://github.com/reddmetrics/forma-clj) forest monitoring project. A cron job runs every morning and grabs new data.

Note that we're not trying to emulate rsync! When MODIS data products are reprocessed (it happens occasionally), new files may appear. Even if you already have data for a specific tile on S3, the new file will also be uploaded because the filenames are different (the processing datestamp changes). It's up to you to make sure having two files for the same period won't cause problems with how you use MODIS data.
