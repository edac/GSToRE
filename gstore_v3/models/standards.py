from lxml import etree
import json

_FORMATS = {
    "tif": "Tagged Image File Format (TIFF)",
    "sid": "Multi-resolution Seamless Image Database (MrSID)",
    "ecw": "ERDAS Compressed Wavelets (ecw)",
    "img": "ERDAS Imagine (img)",
    "zip": "ZIP",
    "shp": "ESRI Shapefile (shp)",
    "kml": "KML",
    "gml": "GML",
    "geojson": "GeoJSON",
    "json": "JSON",
    "csv": "Comma Separated Values (csv)",
    "xls": "MS Excel format (xls)",
    "xlsx": "MS Office Open XML Spreadsheet (xslx)",
    "pdf": "PDF",
    "doc": "MS Word format (doc)",
    "docx": "MS Office Open XML Document (docx)",
    "html": "HTML",
    "txt": "Plain Text",
    "dem": "USGS ASCII DEM (dem)"
}


class FGDC():
    '''
    intermediate fgdc object to handle the modifications 
    to the metadata on each request:

    - update datsetid if geoimage
    - update title to dataset description
    - append gstore onlinks
    - overwrite the distinfo for updated edac info and all the stdorder elements
    - overwrite the metadata contact for updated edac contact
    '''

    def __init__(self, xml, appinfo):
        self.xml = xml
        self.appinfo = appinfo

    def update_datsetid(self, dataset_uuid):
        '''
        if raster, we need the datsetid (required for fgdc-rse)
        so add it if it's missing or overwrite the value if it isn't 
        '''
        idinfo = self.xml.find('idinfo')
        datsetid = idinfo.find('datsetid')
        if datsetid is None:
            datsetid = etree.Element('datsetid')
            idinfo.insert(0, datsetid)
        datsetid.text = dataset_uuid

    def update_title(self, dataset_description):
        '''
        change the title from whatever to the dataset description so that they match across systems (gstore, geoportal, etc)
        '''
        title = self.xml.find('idinfo/citation/citeinfo/title')
        if title is None:
            raise Exception()
        title.text = dataset_description

    def update_onlinks(self, onlinks):
        '''
        strip out any junk/empty onlinks (anything not pointing to a legit url)
        
        '''
        citation = self.xml.find('idinfo/citation/citeinfo')
        if citation is None:
            raise Exception()
        
        #chuck any bad ones or any existing gstore ones
        #TODO: change to chuck any onlinks from the HOST 
        existing_onlinks = citation.findall('onlink')
        if existing_onlinks:
            for existing_onlink in existing_onlinks:
                if existing_onlink is None or (existing_onlink.text and (existing_onlink.text[0:4] != 'http' or 'gstore.unm.edu' in existing_onlink.text)) or not existing_onlink.text:
                    citation.remove(existing_onlink)
        
        #figure out the lworkcit index if it has an lworkcit element
        lworkcit = citation.find('lworkcit')
        cnt = citation.index(lworkcit) if lworkcit is not None else -99
        
        for onlink in onlinks:
            if cnt >= 0:
                link = etree.Element('onlink')
                link.text = onlink
                citation.insert(cnt, link)
                cnt += 1
            else:
                etree.SubElement(citation, 'onlink').text = onlink

    def update_distinfo(self, preferred, downloads):
        '''
        replace the entire distribution info section
        and use the preferred list to append the stdorders in the order we want (first matters in iso/geoportal)
        '''
        distinfo = self.xml.find('distinfo')
        if distinfo is None:
            raise Exception()
            
        for child in distinfo:
            distinfo.remove(child)
            
        distrib = etree.SubElement(distinfo, 'distrib')
        cntinfo = self.build_edac_contact()
        distrib.append(cntinfo)
        
        etree.SubElement(distinfo, 'resdesc').text = 'Downloadable Data'
        etree.SubElement(distinfo, 'distliab').text = 'The material on this site is made available as a apublic service. Maps and data are to be used for reference purposes only and the Earth Data Analysis Center (EDAC), %s and The University of New Mexico are not responsible for any inaccuracies herein contained. No responsibility is assumed for damages or other liabilities due to the accuracy, availability, use or misuse of the information herein provided. Unless otherwise indicated in the documentation (metadata) for individual data sets, information on this site is public domain and may be copied without permission; citation of the source is appreciated.' % self.appinfo.full_name
        
        format = None
        for p in preferred:
            if p in downloads:
                format = p
                break
        
        if format:
            cnt = 3
            canonical_format = downloads[format]
            
            fmt = _FORMATS[format] if format else 'Unknown'
            stdorder = self.build_stdorder(fmt, canonical_format['link'], canonical_format['transize'])
            distinfo.insert(cnt, stdorder)
            cnt += 1
            
            del downloads[format]
            
            for k, v in downloads.iteritems():
                fmt = _FORMATS[k] if k in _FORMATS else 'Unknown'
                stdorder = self.build_stdorder(fmt, v['link'], v['transize'])
                distinfo.insert(cnt, stdorder)
                cnt += 1
                
        etree.SubElement(distinfo, 'custom').text = 'Contact Earth Data Analysis Center at clearinghouse@edac.unm.edu'
        etree.SubElement(distinfo, 'techpreq').text = 'Adequate computer capability is the only technical prerequisite for viewing data in digital form.'
        
    def update_metc(self):
        '''
        update the metadata contact element
        '''
        metc = self.xml.find('metainfo/metc')
        if metc is None:
            raise Exception()
        
        for child in metc:
            metc.remove(child)
        
        cntinfo = self.build_edac_contact()
        metc.insert(0, cntinfo)    
        
    def build_edac_contact(self):
        '''
        just build the cntinfo element and return
        '''    
        cntinfo = etree.Element('cntinfo')
        cntorgp = etree.SubElement(cntinfo, 'cntorgp')
        etree.SubElement(cntorgp, 'cntorg').text = 'Earth Data Analysis Center'
        etree.SubElement(cntinfo, 'cntpos').text = 'Clearinghouse Manager'
        
        cntaddr = etree.SubElement(cntinfo, 'cntaddr')
        etree.SubElement(cntaddr, 'addrtype').text = 'mailing and physical address'
        etree.SubElement(cntaddr, 'address').text = 'MSC01 1110'
        etree.SubElement(cntaddr, 'address').text = '1 University of New Mexico'
        etree.SubElement(cntaddr, 'city').text = 'Albuquerque'
        etree.SubElement(cntaddr, 'state').text = 'NM'
        etree.SubElement(cntaddr, 'postal').text = '87131-0001'
        etree.SubElement(cntaddr, 'country').text = 'USA'
        
        etree.SubElement(cntinfo, 'cntvoice').text = '505-277-3622 ext. 230'
        etree.SubElement(cntinfo, 'cntfax').text = '505-277-3614'
        etree.SubElement(cntinfo, 'cntemail').text = 'clearinghouse@edac.unm.edu'
        etree.SubElement(cntinfo, 'hours').text = '0800 - 1700 MT, M-F -7 hours GMT'
        
        return cntinfo

    def build_stdorder(self, format_name, networkr, transize):
        '''
        build a stdorder element (NOT added to distinfo here)
        '''
        stdorder = etree.Element('stdorder')
        digform = etree.SubElement(stdorder, 'digform')
        digtinfo = etree.SubElement(digform, 'digtinfo')
        etree.SubElement(digtinfo, 'formname').text = format_name
        
        if transize > 0.:
            etree.SubElement(digtinfo, 'transize').text = '%s' % (float('%.1g' % transize) if transize > 1. else 1)
        
        digtopt = etree.SubElement(digform, 'digtopt')
        onlinopt = etree.SubElement(digtopt, 'onlinopt')
        computer = etree.SubElement(onlinopt, 'computer')
        networka = etree.SubElement(computer, 'networka')
        etree.SubElement(networka, 'networkr').text = networkr
        etree.SubElement(onlinopt, 'accinstr').text = self.appinfo.url
        
        etree.SubElement(stdorder, 'fees').text = 'None. The files are available to download from %s.' % self.appinfo.name
        etree.SubElement(stdorder, 'ordering').text = 'Download from %s at %s.' % (self.appinfo.name, self.appinfo.url)
        
        return stdorder


    def update(self, dataset_uuid, dataset_description, taxonomy, onlinks, downloads):
        '''
        add/update datsetid if raster
        update title
        add onlinks
        update distribution info
        update metadata contact
        '''
        
        preferred = ['zip', 'pdf', 'doc', 'docx', 'xls', 'xlsx', 'html', 'txt']
        if taxonomy == 'geoimage':
            self.update_datsetid(dataset_uuid)
            preferred = ['tif', 'img', 'sid', 'ecw', 'dem', 'zip']
        elif taxonomy == 'vector':
            preferred = ['zip', 'shp', 'kml', 'gml', 'geojson', 'json', 'csv', 'xls']
            
            
        self.update_title(dataset_description)    
        self.update_metc()
        self.update_onlinks(onlinks)
        self.update_distinfo(preferred, downloads)  

        #return the updated xml?
        return etree.tostring(self.xml)








