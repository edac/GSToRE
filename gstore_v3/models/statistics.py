from gstore_v3.models import Base, DBSession

import json
from datetime import datetime

'''
models & classes for the statistics (currently app-level stats, see specific model for table-level stats, ie. datasets, etc)

'''

class AppStatistics():
    def __init__(self, app):
        self.app = app

    def __repr__(self):
        return '<AppStatistic %s>' % self.app

    #TODO: update this to have different aggregation levels and possibly date range filtering (datasets added btwn and b)
    def get_dataset_counts_by_dateadded(self, format):
        '''
        return a timeseries of dataset counts by aggregate level

        currently for entire collection, aggregated by month
        '''

        #TODO: convert this to some appropriate sqlalchemy structure? at least optimized a bit if we need to
        #returns a tuple as year, month, count
        sql = "select date_part('year', d.dateadded) as the_year, date_part('month', d.dateadded) as the_month, count(d.id) as datasets from gstoredata.datasets d where '%s' = ANY(d.apps_cache) and d.inactive = False group by the_year, the_month order by the_year, the_month;" % self.app
        result = DBSession.execute(sql)

        #something should be there, but just in case
        if result.rowcount < 1:
            return None

        #repack in the correct format
        if format == 'csv':
            output = 'year,month,count\n'
            for r in result:
                output += '%s,%s,%s\n' % (int(r[0]), int(r[1]), int(r[2]))
            return output
        else:
            return None
        
