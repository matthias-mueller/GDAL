import os, glob, multiprocessing, csv

import osgeo.ogr as ogr
import osgeo.osr as osr

from subprocess import call

suffix = '.xyz'
suffix_len = len(suffix)
cellsize = 2
epsg_code = 25833
no_data = -9999
parallel_threads = 4

def deleteShapeFile(fn_short):
  if os.path.exists(fn_short+".shp"):
    os.remove(fn_short+".shp")
    os.remove(fn_short+".prj")
    os.remove(fn_short+".dbf")
    os.remove(fn_short+".shx")

def processFile(fn):
  if os.path.isfile(fn) & fn.endswith(suffix):
    print (fn)
    fn_short = str(fn[:-suffix_len])

    driver = ogr.GetDriverByName("ESRI Shapefile")
    data_source = driver.CreateDataSource(fn_short+".shp")
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    layer = data_source.CreateLayer(fn_short, srs, ogr.wkbPoint25D)

    with open(fn, 'rb') as f:
      reader = csv.reader(f, delimiter=' ')
      for row in reader:
        row = filter(None, row) #remove null elements (e.g. from duplicate separators)
        feature = ogr.Feature(layer.GetLayerDefn())
        wkt = "POINT(%f %f %f)" % (float(row[0]), float(row[1]), float(row[2]))
        point = ogr.CreateGeometryFromWkt(wkt)
        feature.SetGeometry(point)
        layer.CreateFeature(feature)
        feature.Destroy()

    data_source.Destroy()
    
    # convert to raster and remove shapefile
    cmd = "gdal_rasterize -of GTiff -3d -a_nodata %f -init %f -tr %i %i %s.shp %s.tif" % (no_data, no_data, cellsize, cellsize, fn_short, fn_short)
    print cmd
    return_code = call(cmd, shell=True)
    deleteShapeFile(fn_short)
    return fn_short+".tif"

def main():
  pool = multiprocessing.Pool(processes=parallel_threads)
  files = os.listdir('.')
  rasters = pool.map(processFile, files)
  #for fn in files:
  #  processFile(fn)

  # mosaic all tif to raster
  cmd = 'gdalbuildvrt mosaic_index.vrt *.tif'
  print cmd
  call(cmd, shell=True)
  cmd = 'gdal_translate -co "COMPRESS=DEFLATE" -co "TILED=YES" -co "PREDICTOR=3" -co "BIGTIFF=YES" mosaic_index.vrt mosaic.tif'
  print cmd
  call(cmd, shell=True)
  
  # build overview
  cmd = 'gdaladdo -ro -r average --config COMPRESS_OVERVIEW DEFLATE mosaic.tif 4 8 16 32 64 128 256 512'
  print cmd
  call(cmd, shell=True)
  
  # compute stats
  cmd = 'gdalinfo -stats mosaic.tif'
  print cmd
  call(cmd, shell=True)


if __name__ == "__main__":
  #freeze_support()
  main()
