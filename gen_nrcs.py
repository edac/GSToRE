from gstore.model import meta, Dataset, ShapesVector, VectorDataset
import os

s = """ curl "http://127.0.0.1:9999/apps/epscor/features.%s?dataset_ids=%s" -o /clusterdata/gstore/tmp/%s"""


def first():
    for d in meta.Session.query(Dataset).filter(Dataset.theme == 'Meteorology').filter(Dataset.inactive == False):
        basefilename = d.get_filename('shp')[0:-4]
        #os.system("mkdir /clusterdata/gstore/tmp/%s" % d.id)
        #[os.system("mkdir /clusterdata/gstore/tmp/%s/%s" % (d.id, fmt)) for fmt in ['shp','csv', 'json', 'gml','xls', 'kml']]
        fnamekml = "%s/%s/%s.%s" % (d.id, 'kml', basefilename, 'kml')
        fnamejson = "%s/%s/%s.%s" % (d.id, 'json', basefilename, 'json')
        #if os.path.isfile(fnamekml):
        #   continue
        print s % ('kml', d.id, fnamekml )
        print s % ('json', d.id, fnamejson)
        #os.system(s % ('kml', d.id, fnamekml ))
        #os.system(s % ('json', d.id, fnamejson))

def last():
    for d in meta.Session.query(Dataset).filter(Dataset.theme == 'Meteorology').filter(Dataset.inactive == False):
        basefilename = d.get_filename('shp')[0:-4]
        fnamekml = "%s/%s/%s.%s" % (d.id, 'kml', basefilename, 'kml')
        fnamejson = "%s/%s/%s.%s" % (d.id, 'json', basefilename, 'json')
        print """ogr2ogr -f "ESRI Shapefile" /clusterdata/gstore/tmp/%s/shp/%s.shp /clusterdata/gstore/tmp/%s""" % (
            d.id, 
            d.basename, 
            fnamejson
        )
        print """zip /clusterdata/gstore/tmp/%s/shp/%s.zip /clusterdata/gstore/tmp/%s/shp/*""" % (
            d.id, 
            basefilename, 
            d.id
        )
        print """ogr2ogr -f "CSV" /clusterdata/gstore/tmp/%s/csv/%s /clusterdata/gstore/tmp/%s""" % (
            d.id, 
            d.basename, 
            fnamejson
        )
        print """mv /clusterdata/gstore/tmp/%s/csv/%s/OGRGeoJSON.csv /clusterdata/gstore/tmp/%s/csv/%s.csv""" % ( 
            d.id, 
            d.basename, 
            d.id, 
            d.basename
        )
        print """zip /clusterdata/gstore/tmp/%s/csv/%s.csv.zip /clusterdata/gstore/tmp/%s/csv/%s.csv""" % (
        d.id,
        basefilename,
        d.id,
        d.basename
    )

        print "mv /clusterdata/gstore/tmp/%s /clusterdata/gstore/tmp/%s/kml/%s.kml" % (
            fnamekml,
            d.id, 
            d.basename 
        )
        print """zip /clusterdata/gstore/tmp/%s/kml/%s.kmz /clusterdata/gstore/tmp/%s/kml/%s.kml""" % (
        d.id, 
        basefilename,
        d.id,   
        d.basename
        )   
        print "mv /clusterdata/gstore/tmp/%s /clusterdata/gstore/tmp/%s/json/%s.json" % (
            fnamejson,
            d.id, 
            d.basename 
        )
        print """zip /clusterdata/gstore/tmp/%s/json/%s.json.zip /clusterdata/gstore/tmp/%s/json/%s.json""" % (
        d.id, 
        basefilename,
        d.id,   
        d.basename
        )   
 
last() 
