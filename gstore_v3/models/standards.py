from lxml import etree
import os, json
from datetime import datetime
from copy import deepcopy

from ..lib.spatial import epsg_to_sr

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

#NOTE: there probably isn't a good reason for this to be separate from the actual model 
#      now that we've decided not to update fgdc/iso/whatever independently. doesn't hurt,
#      just not so necessary.
class GstoreMetadata():
    '''
    for xml using the gstore schema
    '''

    date_format = '%Y-%m-%d'
    time_format = '%H:%M:%S'

    def __init__(self, xml):
        #as an actual lxml.etree object, not just text
        self.xml = xml

    '''
    update the gstore metadata stored internally

    - update the title (citation.identify & title)
    - update the metadata contact info - add new contact (for edac) and update the refs for metadata and distribution
    - update the metadata pub date
    - update the onlinks (citation.identify)
    - add distribution info (with new contact if necessary)
    - update spatial refs IF FGDC AND TYPE != UNKNOWN
    
    this will potentially do bad things to the gstore xml but it's ephemeral
    '''    

    def get_as_text(self):
        return etree.tostring(self.xml, pretty_print=True) 

    def update_xml(self, elements_to_update, out_standard, spatialref_file=''):
        '''
        elements_to_update:
            distribution_info:                
                liability: liability string (for fgdc)
                access: access constraints (for fgdc)
                fees: fees (for fgdc) (for iso)
                ordering: ordering info (for fgdc) (for iso)
                prereqs: technica prerequisites (for fgdc)
                description: resdesc (for fgdc)
                links: [{"type": , "size": , "link": }, ..]

            onlinks
            metadata_contact
            title
        '''

        title = elements_to_update['title'] if 'title' in elements_to_update else ''
        if title:
            #just deal with the ampersands
            title = title.replace('&', '&amp;') if '&amp;' not in title and '&' in title else title
            self.update_title(title)

        #check for dataset and drop that (it's in all the migrated data)
        self.drop_dataset_attribute()

        identifier = elements_to_update['identifier'] if 'identifier' in elements_to_update else ''
        if identifier:
            self.update_identifier(identifier)

        self.update_metadata_pubdate()
        self.update_metadata_contact()

        onlinks = elements_to_update['onlinks'] if 'onlinks' in elements_to_update else []
        base_url = elements_to_update['base_url'] if 'base_url' in elements_to_update else ''
        if onlinks and base_url:
            self.update_onlinks(onlinks, base_url)

        if 'distribution' in elements_to_update:
            self.update_distribution(elements_to_update['distribution'])
        else:
            #drop the distribution completely so nothing wonky gets through
            self.drop_distribution()

        if 'pubdate' in elements_to_update:
            self.update_pubdate(elements_to_update['pubdate'])    

        if 'timeperiod' in elements_to_update:
            self.update_timeperiod(elements_to_update['timeperiod'])

        if 'bbox' in elements_to_update:
            self.update_bbox(elements_to_update['bbox'])    

        #only for ds records though
        if 'dataset_links' in elements_to_update:
            self.update_datasets(elements_to_update['dataset_links'])

        #TODO: re-enable this once the sprefs are fixed
        if ('fgdc' in out_standard.lower() or 'gstore' in out_standard.lower()) and spatialref_file:
            self.update_spatial_refs(spatialref_file)

        pubs = elements_to_update['publications'] if 'publications' in elements_to_update else []
        if pubs:
            self.update_dataset_citations(pubs)


    def update_identifier(self, identifier):
        ident = self.xml.find('identification')
        ident.attrib['identifier'] = identifier        

    def drop_dataset_attribute(self):
        ident = self.xml.find('identification')
        if 'dataset' in ident.attrib:
            del ident.attrib['dataset']

    def update_title(self, title):
        title_element = self.xml.find('identification/title')
        if title_element is not None:
            title_element.text = title

        #and the citation title
        id_citation_element = self.xml.xpath('identification/citation[@role="identify"]')
        if id_citation_element is None:
            return 
        id_citation_ref = id_citation_element[0].attrib['ref']
        title_element = self.xml.xpath('citations/citation[@id="%s"]/title' % id_citation_ref)
        if title_element:
            title_element[0].text = title

    def update_pubdate(self, pubdate):
        '''
        update the identity citation's publication date/time

        for gstore, this would be the object's date added date time (published in the system)
        '''
        id_citation_element = self.xml.xpath('identification/citation[@role="identify"]')
        if id_citation_element is None:
            return 
        id_citation_ref = id_citation_element[0].attrib['ref']
        citation_element = self.xml.xpath('citations/citation[@id="%s"]' % id_citation_ref)

        pubdate_element = citation_element[0].find('publication/pubdate')
        if pubdate_element is None:
            publication_element = citation_element.find('publication')
            pubdate_element = etree.Element('pubdate')           
            publication_element.insert(0, pubdate_element)

        pubdate_date, pubdate_time = self.parse_date_and_time(pubdate)
        pubdate_element.attrib['date'] = pubdate_date
        if pubdate_time:
            pubdate_element.attrib['time'] = pubdate_time

    def update_bbox(self, bbox):
        '''
        bbox as [minx,miny,maxx,maxy]
        '''

        spatial_element = self.xml.find('spatial')
        if spatial_element is None:
            #sibling element order: identification, constraints, spatial
            prev_sib_element = self.xml.find('constraints')
            if prev_sib_element is None:
                #and this has to be there or it's invalid gstore metadata
                prev_sib_element = self.xml.find('identification')
                
            index = self.xml.index(prev_sib_element)
            spatial_element = etree.Element('spatial')
            self.xml.insert(index+1, spatial_element)

            etree.SubElement(spatial_element, 'west')
            etree.SubElement(spatial_element, 'east')
            etree.SubElement(spatial_element, 'south')
            etree.SubElement(spatial_element, 'north')
  
        west = spatial_element.find('west')
        west.text = str(bbox[0])

        east = spatial_element.find('east')
        east.text = str(bbox[2])

        south = spatial_element.find('south')
        south.text = str(bbox[1])

        north = spatial_element.find('north')
        north.text = str(bbox[3])
            

    def update_timeperiod(self, date_info):
        '''
        {
            "single": 
        }

        OR 

        {
            "start":
            "end":
        }

        OR 

        {
            "multiple": []
        }

        which i don't actually know how we would do from the db. 
        '''

        time_element = self.xml.find('identification/time')
        if time_element is None:
            return ''

        #make sure it's not already multiple dates (just keep those)
        single_elements = time_element.xpath('single')    
        if len(single_elements) > 1:
            return ''

        #chuck what's there except for the current element
        for child in time_element:
            if child.tag != 'current':
                time_element.remove(child)

        if 'single' in date_info:
            single = etree.Element('single')
            single_date, single_time = self.parse_date_and_time(date_info['single'])    

            single.attrib['date'] = single_date
            if single_time:
                single.attrib['time'] = single_time

            time_element.insert(0, single)    
            
        elif 'start' in date_info and 'end' in date_info:
            start_date, start_time = self.parse_date_and_time(date_info['start'])
            start = etree.Element('start')
            start.attrib['date'] = start_date
            if start_time:
                start.attrib['time'] = start_time

            end_date, end_time = self.parse_date_and_time(date_info['end'])
            end = etree.Element('end')
            end.attrib['date'] = end_date
            if end_time:
                end.attrib['end'] = end_time

            range_element = etree.Element('range')
            range_element.append(start)
            range_element.append(end)

            time_element.insert(0, range_element)
        elif 'multiple' in date_info:
            #really not sure how we would get multiple datetimes from anywhere in gstore
            pass
            

    def parse_date_and_time(self, dt):
        '''
        need to handle the pre-1900 datasets to make sure we're emitting valid metadata (stupid prism)

        '''
        if not isinstance(dt, datetime) and dt.lower() == 'unknown':
            return 'Unknown', ''

        #otherwise, return the formatted strings    
        d = ''
        try:
            d = dt.strftime(self.date_format) if dt.year >= 1900 else '%s%02d%02d' % (dt.year, dt.month, dt.day)
        except:
            d = 'Unknown'

        #TODO: check on the < 1900 datetime with TIME for this
        t = ''
        try:
            t = dt.strftime(self.time_format)
        except:
            t = '' 

        return d, t

    def update_onlinks(self, onlinks, base_url):
        #TODO: we should have a better way of dealing with the gstore onlinks 
        #add to the identify citation
        id_citation_element = self.xml.xpath('identification/citation[@role="identify"]')
        if id_citation_element is None:
            return 
        id_citation_ref = id_citation_element[0].attrib['ref']
        citation_element = self.xml.xpath('citations/citation[@id="%s"]' % id_citation_ref)

        #strip out any existing gstore onlinks
        if not citation_element:
            #this is a problem if we want to include the onlinks. do we build a citation for edac? 
            return
        citation_element = citation_element[0]
        existing_onlinks = citation_element.findall('onlink')
        for existing_onlink in existing_onlinks:
            if existing_onlink is None or (
                    existing_onlink.text and (
                            existing_onlink.text[0:4] != 'http' or
                            base_url in existing_onlink.text or
                            base_url.replace('https', 'http') in existing_onlink.text
                       )
                    ) or not existing_onlink.text:
                citation_element.remove(existing_onlink)

        #add the new ones
        for onlink in onlinks:
            etree.SubElement(citation_element, 'onlink').text = onlink

    def update_datasets(self, datasets):
        dataset_element = self.xml.find('datasets')

        if dataset_element is None:
            dataset_element = etree.SubElement(self.xml, 'datasets')
            
        #remove any children if there are any
        existing_datasets = dataset_element.findall('dataset')
        for existing_dataset in existing_datasets:
            dataset_element.remove(existing_dataset)

        #and add the new
        for d in datasets:
            etree.SubElement(dataset_element, 'dataset').text = d


    def update_metadata_pubdate(self):
        #update the metadata publication date for this new version (i.e. the version we're generating now)
        pubdate_element = self.xml.find('metadata/pubdate')

        new_date = datetime.now()

        pubdate_element.attrib['date'] = new_date.strftime(self.date_format)
        pubdate_element.attrib['time'] = new_date.strftime(self.time_format)    

    #TODO: replace this with a contact from the database
    def update_metadata_contact(self):
        '''
        just to be sure, insert a new contact and change the metadata contact ref to the new contact id
        
        ''' 
        new_id = 'gstore-100'
        new_contact = etree.Element('contact')
        new_contact.attrib['id'] = new_id    
        etree.SubElement(new_contact, 'position').text = 'Clearinghouse Manager'   
        org = etree.SubElement(new_contact, 'organization')
        etree.SubElement(org, 'name').text = 'Earth Data Analysis Center'

        address = etree.SubElement(new_contact, 'address')
        address.attrib['type'] = 'mailing and physical address'
        etree.SubElement(address, 'addr').text = 'MSC01 1110'
        etree.SubElement(address, 'addr').text = '1 University of New Mexico'
        etree.SubElement(address, 'city').text = 'Albuquerque'
        etree.SubElement(address, 'state').text = 'NM'
        etree.SubElement(address, 'postal').text = '87131-0001'
        etree.SubElement(address, 'country').text = 'USA'

        etree.SubElement(new_contact, 'voice').text = '505-277-3622 ext. 230'
        etree.SubElement(new_contact, 'fax').text = '505-277-3614'
        etree.SubElement(new_contact, 'email').text = 'clearinghouse@edac.unm.edu'
        etree.SubElement(new_contact, 'hours').text = '0800 - 1700 MT, M-F -7 hours GMT'

        #append the new contact
        contacts = self.xml.find('contacts')
        contacts.append(new_contact)        

        #and update the metadata contact
        metadata_contact_element = self.xml.xpath('metadata/contact[@role="point-contact"]')
        metadata_contact_element[0].attrib['ref'] = new_id

    def drop_distribution(self):
        distribution_element = self.xml.find('distribution')
        if distribution_element is not None:
            parent = distribution_element.getparent()
            parent.remove(distribution_element)
    
    def update_distribution(self, distribution_info):
        '''
        liability: liability string (for fgdc)
        access: access constraints (for fgdc)
        fees: fees (for fgdc) (for iso)
        ordering: ordering info (for fgdc) (for iso)
        prereqs: technica lprerequisites (for fgdc)
        description: resdesc (for fgdc)
        links: [{"type": , "size": , "link": }, ..]

        output xml:
        <distribution>
              <distributor>
                 <contact role="distributor" id=""></contact>
                 <liability>none</liability>
                 <instructions>none</instructions>
                 <fees>none</fees>
                 <ordering>order stuff</ordering>
                 <prereqs>none</prereqs>
                 <access>go to town</access>
                 <description>none</description>
                 <downloads>
                    <download>
                       <type>TIFF</type>
                       <size>60</size>
                       <link>some url here</link>
                    </download>
                 </downloads>
              </distributor>
           </distribution>
        '''

        if not distribution_info:
            return ''

        distribution_element = self.xml.find('distribution')
        if distribution_element is not None:
            #chuck everything for now
            for child in distribution_element:
                distribution_element.remove(child)
        else:
            distribution_element = etree.Element('distribution')

            metadata_element = self.xml.find('metadata')
            index = self.xml.index(metadata_element)
            self.xml.insert(index+1, distribution_element)
        
        distributor = etree.SubElement(distribution_element, 'distributor')

        #add in the contact
        new_id = 'gstore-200'
        new_contact = etree.Element('contact')
        new_contact.attrib['id'] = new_id    
        etree.SubElement(new_contact, 'position').text = 'Clearinghouse Manager'   
        org = etree.SubElement(new_contact, 'organization')
        etree.SubElement(org, 'name').text = 'Earth Data Analysis Center'

        address = etree.SubElement(new_contact, 'address')
        address.attrib['type'] = 'mailing and physical address'
        etree.SubElement(address, 'addr').text = 'MSC01 1110'
        etree.SubElement(address, 'addr').text = '1 University of New Mexico'
        etree.SubElement(address, 'city').text = 'Albuquerque'
        etree.SubElement(address, 'state').text = 'NM'
        etree.SubElement(address, 'postal').text = '87131-0001'
        etree.SubElement(address, 'country').text = 'USA'

        etree.SubElement(new_contact, 'voice').text = '505-277-3622 ext. 230'
        etree.SubElement(new_contact, 'fax').text = '505-277-3614'
        etree.SubElement(new_contact, 'email').text = 'clearinghouse@edac.unm.edu'
        etree.SubElement(new_contact, 'hours').text = '0800 - 1700 MT, M-F -7 hours GMT'

        #append the new contact
        contacts = self.xml.find('contacts')
        contacts.append(new_contact)   

        distributor_contact = etree.SubElement(distributor, 'contact')
        distributor_contact.attrib['role'] = 'distributor'
        distributor_contact.attrib['ref'] = new_id

        #and all the rest
        etree.SubElement(distributor, 'liability').text = distribution_info['liability']
        etree.SubElement(distributor, 'instructions').text = distribution_info['instructions']
        etree.SubElement(distributor, 'fees').text = distribution_info['fees']
        etree.SubElement(distributor, 'ordering').text = distribution_info['ordering']
        etree.SubElement(distributor, 'prereqs').text = distribution_info['prereqs']
        etree.SubElement(distributor, 'access').text = distribution_info['access']
        etree.SubElement(distributor, 'description').text = distribution_info['description']

        downloads = etree.SubElement(distributor, 'downloads')
        for link in distribution_info['links']:
            linktype = link['type']
            linksize = link['size']
            linkurl = link['link']

            download = etree.SubElement(downloads, 'download')
            etree.SubElement(download, 'type').text = linktype
            if linksize > 0:
                #ignore the size if it's unknown (-99). it's valid without
                etree.SubElement(download, 'size').text = str(linksize)
            etree.SubElement(download, 'link').text = linkurl

    def update_dataset_citations(self, citations):
        '''
        as in publication citations

        append to the end of supplemental info:

        supplinf | pub a | pub b | etc
        '''

        supplinf = self.xml.find('identification/supplinfo')
        if supplinf is None:
            idinfo = self.xml.find('identification')
            purpose = idinfo.find('purpose')
            index = idinfo.index(purpose)
            supplinf = etree.Element('supplinfo')
            idinfo.insert(index+1, supplinf)

        txt = supplinf.text
        txt = ' | '.join([txt] + [p.full_citation for p in citations]) 
        supplinf.text = txt


    def update_spatial_refs(self, fgdc_refs=''):
        '''
        FOR FGDC ONLY
        
        - get the sprefs if type != unknown
        - if no sprefs, bail
        - start with datum/ellipsoid
        - then projections and grid systems
        - then vertical datums if exist
        '''

        #TODO: check for a representation element first - if there is one, skip this?

        known_sprefs = self.xml.xpath('spatial/sprefs/spref[@type!="Unknown"]')
        if not known_sprefs:
            return

        #get the mapproj or gridsys ref
        spref = self.xml.xpath('spatial/sprefs/spref[@type="mapproj" or @type="gridsys"]')


        #if no mapproj/gridsys, get the datum
        if not spref:
            spref = self.xml.xpath('spatial/sprefs/spref[@type="datum"]')

        #if not datum, bail
        if not spref:
            return

        spref = spref[0]

        #make sure we have the ref file
        if fgdc_refs and not os.path.isfile(fgdc_refs):
            return

        #do stuff
        spref_code = spref.find('code').text
        spref_auth = spref.find('authority').text
        
        ref_xml = etree.parse(fgdc_refs)
        ref = ref_xml.xpath('spref[code="%s:%s"]' % (spref_auth, spref_code))
        
        if not ref:
            return

        ref = ref[0]

        ref_def = ref.find('def')

        #add the representation to the sprefs element (it is the fgdc structure to use based on all of the supplied and known spref elements)
        sprefs = self.xml.find('spatial/sprefs')
        representation = etree.SubElement(sprefs, 'representation')
        representation.attrib['type'] = 'fgdc'
        #move the structure over from our file of prebuilt fgdc blobs
        representation.append(deepcopy(ref_def))

    #TODO: these are silly, do something else to get the elements
    def get_abstract(self):
        abstract = self.xml.find('identification/abstract')
        return '' if abstract is None else abstract.text

    def get_isotopic(self):
        isotopic = self.xml.find('identification/isotopic')
        return '' if isotopic is None else isotopic.text

    def get_keywords(self):
        terms = self.xml.xpath('identification/themes/theme/term')
        return [t.text for t in terms]

#####################TO TEST
'''
from gstore_v3.models import *
from lxml import etree
m = DBSession.query(metadata.DatasetMetadata).filter(metadata.DatasetMetadata.id==11).first()
txt = str(m.gstore_xml)
xml = etree.fromstring(str(m.gstore_xml))
title = 'my new title'
base_url = 'http://129.24.63.115'
onlinks = ['http:/129.24.63.115/apps/rgis/link1', 'http:/129.24.63.115/apps/rgis/link2', 'http:/129.24.63.115/apps/rgis/link3']

xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts/spatialrefs.xml'


gm = standards.GstoreMetadata(xml)
gm.update_title(title)
gm.update_metadata_pubdate()
gm.update_metadata_contact()
gm.update_onlinks(onlinks, base_url)

gm.update_spatial_refs(xslt_path)


distribution_info = {"liability": "None.", "fees": "None.", "instructions": "Contact EDAC", "ordering": "download from rgis", "prereqs": "a computer", "access": "None.", "description": "downloadable data", "links":[{"t
ype": "TIFF", "size": "100", "link": "http://gstore.unm.edu/apps/rgis/link1"}, {"type": "MrSID", "size": "25", "link": "http://gsotre.unm.edu/apps/rgis/link2"}]}
gm.update_distribution(distribution_info)


print etree.tostring(gm.xml, pretty_print=True)


test datasets


'''


        
    

