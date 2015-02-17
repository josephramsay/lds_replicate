'''
v.0.0.9

LDSReplicate -  LDSUtilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Simple LDS specific utilities class

Created on 9/08/2012

@author: jramsay
'''
# for windows lxml binary from here http://www.lfd.uci.edu/~gohlke/pythonlibs/#lxml

import re
import os
import logging
import ast

from string import whitespace
from urllib2 import urlopen, build_opener, install_opener, ProxyHandler
from contextlib import closing
from StringIO import StringIO
from lxml import etree

#ldslog = LDSUtilities.setupLogging()
mainlog = 'DEBUG'
ldslog = logging.getLogger(mainlog)

class LDSUtilities(object):
    '''Does the LDS related stuff not specifically part of the datastore''' 
    
    
    LDS_VX_PREFIX = 'v:x'
    #wfs2.0 prefixes
    LDS_LL_PREFIX = 'linz:layer-'
    LDS_DL_PREFIX = 'data.linz.govt.nz:layer-'
    LDS_DT_PREFIX = 'data.linz.govt.nz:table-'
    LDS_DX_PREFIX = 'data.linz.govt.nz:'
    LDS_ME_PREFIX = 'mfe:layer-'
    LORT = ['table','layer'] #variations on idp for finding layer/table names in LC
    
    LDS_PREFIXES = (LDS_VX_PREFIX,LDS_LL_PREFIX,LDS_DL_PREFIX,LDS_DT_PREFIX,LDS_ME_PREFIX)
    
    
    @staticmethod
    def getLDSIDPrefix(ver,svc):
        from lds.DataStore import UnsupportedServiceException
        if svc=='WFS':
            if ver in ('1.0.0','1.1.0','1.0','1.1'):
                return LDSUtilities.LDS_VX_PREFIX
            elif ver in ('2.0.0','2.0'):
                #return LDSUtilities.LDS_LL_PREFIX
                return LDSUtilities.LDS_DX_PREFIX
            else:
                raise UnsupportedServiceException('Only WFS versions 1.0, 1.1 and 2.0 are supported')
        else:
            raise UnsupportedServiceException('Only WFS is supported at present')
    
    @staticmethod
    def adjustWFS2URL(url,ver):
        if ver == '2.0.0':
            url = re.sub('wfs.','',url)+'services;key='
            ldslog.warn('\'wfs.\' deleted from URL to comply with LDS WFS2.0 requirements')
        return url
    
    @staticmethod
    def splitLayerName(layername):
        '''Splits a layer name typically in the format v:x### into /v/x### for URI inclusion'''
        #return "/"+"/".join(layername.split(":"))
        return "/"+re.sub(":","/",layername)
    
    LDS_VXPATH = splitLayerName.__func__(LDS_VX_PREFIX)
    LDS_LLPATH = splitLayerName.__func__(LDS_LL_PREFIX)#?
    LDS_MEPATH = splitLayerName.__func__(LDS_ME_PREFIX)#?
    
    LDS_IDPATHS = (LDS_VXPATH,LDS_LLPATH,LDS_MEPATH)
    
    @staticmethod
    def cropChangeset(layername):
        '''Removes changeset identifier from layer name'''
        return layername.rstrip("-changeset")
    
    @staticmethod
    def checkDateFormat(xdate):
        '''Checks a date parameter conforms to yyyy-MM-ddThh:mm:ss format'''       
        #why not just use... datetime.strptime(xdate,'%Y-%m-%dT%H:%M:%S')
        if type(xdate) is str:
            if re.search('^\d{4}\-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$',xdate):
                return xdate
            elif re.search('^\d{4}\-\d{2}-\d{2}$',xdate):
                return xdate+"T00:00:00"
        return None

    # 772 time test string
    # http://wfs.data.linz.govt.nz/ldskey/v/x772-changeset/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=v:x772-changeset&viewparams=from:2012-09-29T07:00:00;to:2012-09-29T07:30:00&outputFormat=GML2
    
    @staticmethod
    def checkLayerName(lconf,lname):
        '''Makes sure a layer name conforms to v:x format which exists or matches a layername'''
        from lds.DataStore import InvalidLayerException
        if type(lname) is str:
            if LDSUtilities.checkLayerNameValidity(lname) and lname in lconf.getLayerNames():
                #if its an ID (v:x etc) and it matches a configured id return it
                return lname
            elif lconf.findLayerIdByName(lname):
                #if its a name (NZ Special Points) return matching ID
                return lconf.findLayerIdByName(lname)
            else:
                raise InvalidLayerException('Cannot find Layer, '+str(lname))
        else: 
            raise InvalidLayerException('Layer name not a string, '+str(lname))
        return None
         
    @staticmethod
    def checkLayerNameValidity(lname):
        '''check whether provided layer name is v:x, linz:layer- or mfe:layer-'''
        return True if [x for x in LDSUtilities.LDS_PREFIXES if re.search('^{}\d+$'.format(x),lname)] else False
    
    
    @staticmethod
    def interceptSystemProxyInfo(proxyinfo,sys_ref):
        
        (ptype, host, port, auth, usr, pwd) = proxyinfo
        
        if LDSUtilities.mightAsWellBeNone(ptype) == sys_ref:
            #system, read from env/reg
            if os.name == 'nt':
                #windows
                from lds.WinUtilities import Registry as WR
                (_,host,port) = WR.readProxyValues()
            else:
                #unix etc
                hp = os.environ['http_proxy']
                rm = re.search('http://([a-zA-Z0-9_\.\-]+):(\d+)',hp)
                host = rm.group(1)
                port = rm.group(2)
                
        return {'TYPE':ptype, 'HOST':host, 'PORT':port, 'AUTH':auth, 'USR':usr, 'PWD':pwd}

    
    @staticmethod
    def getLayerNameFromURL(url):
        '''checks for both /v/xNNN and v:xNNN occurrences and whether they're the same'''
        from DataStore import MalformedConnectionString
        l1 = [re.search(x+'(\d+)',url,flags=re.IGNORECASE) for x in LDSUtilities.LDS_IDPATHS if re.search(x+'(\d+)',url,flags=re.IGNORECASE)][0]
        l2 = [re.search('typeName='+x+'(\d+)',url,flags=re.IGNORECASE) for x in LDSUtilities.LDS_PREFIXES if re.search('typeName='+x+'(\d+)',url,flags=re.IGNORECASE)][0]
        if l1 is None or l2 is None:
            raise MalformedConnectionString('Cannot extract correctly formatted layer strings from URI')
        else:
            l1 = l1.group(1)
            l2 = l2.group(1)
            if l1!=l2:
                raise MalformedConnectionString('Layer specifications in URI differ; '+str(l1)+'!='+str(l2))
        pref = [x for x in LDSUtilities.LDS_PREFIXES if re.search('typeName='+x+'(\d+)',url,flags=re.IGNORECASE)][0]
        return pref+str(l1)
        
    @staticmethod
    def checkHasChangesetIdentifier(url):
        '''Check whether URL contains changeset id'''
        c1 = [x for x in LDSUtilities.LDS_IDPATHS if re.search(x+'\d+-changeset',url,flags=re.IGNORECASE)]
        c2 = [x for x in LDSUtilities.LDS_PREFIXES if re.search('typeName='+x+'\d+-changeset',url,flags=re.IGNORECASE)]
        return True if c1 and c2 else False#return c1 is not None and c2 is not None
    
    @staticmethod
    def getDateStringFromURL(fort,url):
        '''Parse a selected date string from a user supplied URL. Can return none as that would indicate non incremental'''
        #yeah thats right, ForT = F(rom) or T(o)
        udate = re.search(fort+':(\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})*)',url)
        return udate


    @staticmethod
    def xmlEscape(url):
        '''Simple XML escaping regex used to properly format WFS URLS (wfs specs ask for it but it doesn't seem to be needed)'''
        #first 4, simple replace: "=&quot; '=&apos; <=&lt; >=&gt; &=&amp;
        url = re.sub('"','&quot;',url)
        url = re.sub('\'','&apos;',url)
        url = re.sub('<','&lt;',url)
        url = re.sub('>','&gt;',url)
        #them match & but not anything that has already been escaped
        #the original string could also contain escaped chars so we have to do skip escapes anyway 
        return re.sub('&(?!amp;|apos;|quot;|lt;|gt;)','&amp;',url)    
    
    
    @staticmethod
    def percentEncode(url):
        '''Simple http bracket/comma escaping regex used to properly format WFS URLS'''
        #this is the full list but we should only need a small subset of these, i.e. brackets, spaces and commas
        #      !     #     $     &     '     (     )     *     +     ,     /     :     ;     =     ?     @     [     ]
        #%20   %21   %23   %24   %26   %27   %28   %29   %2A   %2B   %2C   %2F   %3A   %3B   %3D   %3F   %40   %5B   %5D
        fpe = {' ':'%20','!':'%21','#':'%23','$':'%24','&':'%26',"'":'%27','\(':'%28','\)':'%29','\*':'%2A','\+':'%2B',',':'%2C','/':'%2F',':':'%3A',';':'%3B','=':'%3D','\?':'%3F','@':'%40','\[':'%5B','\]':'%5D'}
        rpe = {' ':'%20','\(':'%28','\)':'%29',',':'%2C'}
        for k in rpe:
            url = re.sub(k,rpe[k],url)
        #url = re.sub('\(','%28',url)
        #url = re.sub('\)','%29',url)
        #url = re.sub(',','%2C',url)
        #url = re.sub(' ','%20',url)
        return url
    
    @staticmethod
    def reVersionURL(url,newversion='1.1.0'):
        '''Because there is sometimes a problem with WFS <1.0.0, esp GetFeatureCount, change to WFS 1.1.0'''
        ldslog.warn('Rewriting URI version to '+str(newversion))
        return re.sub('&version=[0-9\.]+','&version='+str(newversion),url)
    
    
    @staticmethod
    def containsOnlyAlphaNumeric(anstr):
        '''Checks for non alphnumeric characters in a string, for schema/table name testing'''
        #also allows underscore
        return re.search('[^a-zA-Z0-9_]',anstr) is None

    @staticmethod
    def checkCQL(cql):
        '''Since CQL commands are freeform strings we need to try and validate at least the most basic errors. This is very simple
        RE matcher that just looks for valid predicates... for now. Won't stop little Bobby Tables
        
        <predicate> ::= <comparison predicate> | <text predicate> | <null predicate> | <temporal predicate> | timestamp merge<classification predicate> | <existence_predicate> | <between predicate> | <include exclude predicate>
        
        LDS expects the following;
        Was expecting one of:
            "not" ...
            "include" ...
            "exclude" ...
            "(" ...
            "[" ...
            "id" ...
            "in" ...
            <IDENTIFIER> ...
            "-" ...
            <INTEGER_LITERAL> ...
            <FLOATING_LITERAL> ...
            <STRING_LITERAL> ...
            <STRING_LITERAL> "*" ...
            <STRING_LITERAL> "/" ...
            <STRING_LITERAL> "+" ...
            <STRING_LITERAL> "-" ...
            <STRING_LITERAL> "not" ...
            <STRING_LITERAL> "like" ...
            <STRING_LITERAL> "exists" ...
            <STRING_LITERAL> "does-not-exist" ...
            <STRING_LITERAL> "is" ...
            <STRING_LITERAL> "between" ...
            <STRING_LITERAL> "before" ...
            <STRING_LITERAL> "after" ...
            <STRING_LITERAL> "during" ...
            <STRING_LITERAL> "=" ...
            <STRING_LITERAL> ">" ...
            <STRING_LITERAL> "<" ...
            <STRING_LITERAL> ">=" ...
            <STRING_LITERAL> "<=" ...
            <STRING_LITERAL> "<>"
        '''
        v = 0
        
        #comp pred
        if re.match('.*(?:!=|=|<|>|<=|>=)',cql):
            v+=1
        #text pred
        if re.match('.*(?:not\s*)?like.*',cql,re.IGNORECASE):
            v+=2
        #null pred
        if re.match('.*is\s*(?:not\s*)?null.*',cql,re.IGNORECASE):
            v+=4
        #time pred
        if re.match('.*(?:before|during|after)',cql,re.IGNORECASE):
            v+=8
        #clsf pred, not defined
        #exst pred
        if re.match('.*(?:does-not-)?exist',cql,re.IGNORECASE):
            v+=32
        #btwn pred
        if re.match('.*(?:not\s*)?between',cql,re.IGNORECASE):
            v+=64
        #incl pred
        if re.match('.*(?:include|exclude)',cql,re.IGNORECASE):
            v+=128
        #geo predicates just for good measure, returns v=16 overriding classification pred
        if re.match('.*(?:equals|disjoint|intersects|touches|crosses|within|contains|overlaps|bbox|dwithin|beyond|relate)',cql,re.IGNORECASE):
            v+=16
            
        ldslog.debug("CQL check:"+cql+":"+str(v))
        if v>0:
            return cql
        else:
            return ""
        
    @staticmethod    
    def precedence(first,second,third):
        '''Decide which CQL filter to apply based on scope and availability'''
        '''Generally assume; CommandLine > Config-File > Layer-Properties but maybe its better for individual layers to override a global setting... '''
        
        if LDSUtilities.mightAsWellBeNone(first) is not None:
            return first
        elif LDSUtilities.mightAsWellBeNone(second) is not None:
            return second
        elif LDSUtilities.mightAsWellBeNone(third) is not None:
            return third
        return None
    
    @staticmethod
    def extractFields(feat):
        '''Extracts named fields from a layer config feature'''
        '''Not strictly independent but common and potentially used by a number of other classes'''
        
        try:
            id =  feat.GetField('ID')
        except:
            ldslog.debug("LayerSchema: Can't read Feature ID")
            id = None
            
        try:
            pkey =  feat.GetField('PKEY')
        except:
            ldslog.debug("LayerSchema: No Primary Key Column defined, default to 'ID'")
            pkey = 'ID'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = feat.GetField('NAME')
        except:
            ldslog.debug("LayerSchema: No Name saved in config for this layer, returning ID")
            name = None
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            group = feat.GetField('CATEGORY')
        except:
            ldslog.debug("Group List: No Groups defined for this layer")
            group = None
                  
        try:
            gcol = feat.GetField('GEOCOLUMN')
        except:
            ldslog.debug("LayerSchema: No Geo Column defined, default to 'SHAPE'")
            gcol = 'SHAPE'
            
        try:
            index = feat.GetField('INDEX')
        except:
            ldslog.debug("LayerSchema: No Index Column/Specification defined, default to None")
            index = None
            
        try:
            epsg = feat.GetField('EPSG')
        except:
            #print "No Projection Transformation defined"#don't really need to state the default occurance
            epsg = None
            
        try:
            lmod = feat.GetField('LASTMODIFIED')
        except:
            ldslog.debug("LayerSchema: No Last-Modified date recorded, successful update will write current time here")
            lmod = None
            
        try:
            disc = feat.GetField('DISCARD')
        except:
            disc = None 
            
        try:
            cql = feat.GetField('CQL')
        except:
            cql = None
            
        
        return LayerConfEntry(id,pkey,name,group,gcol,epsg,lmod,disc,cql)
    
    @staticmethod
    def standardiseDriverNames(dname=''):
        '''Returns standard identifier (defined by DRIVER_NAME) for different dests'''
        dname = dname.lower()
        from DataStore import DataStore
        if re.match('pg|postgres',dname):
            return DataStore.DRIVER_NAMES['pg']
        elif re.match('ms|microsoft|sqlserver',dname):
            return DataStore.DRIVER_NAMES['ms']
        elif re.match('sl|sqlite|spatialite',dname):
            return DataStore.DRIVER_NAMES['sl']
        elif re.match('fg|filegdb|esri',dname):
            return DataStore.DRIVER_NAMES['fg']
        elif re.match('wfs|lds',dname):
            #since a user could ask for lds meaning wfs though this will have to change if we implement wms etc TODO
            from lds.WFSDataStore import WFSDataStore
            return WFSDataStore.DRIVER_NAME
        return None
    
    
    @staticmethod
    def readDocument(url,proxy=None):
        '''Non-Driver method for fetching LDS DS as a document'''
        ldslog.debug("LDS URL {} Pxy {}".format(url,proxy))
        if LDSUtilities.isProxyValid(proxy): install_opener(build_opener(ProxyHandler(proxy)))
        with closing(urlopen(url)) as lds:
            data = lds.read()
        return data
    
    @staticmethod
    def isProxyValid(pxy):
        '''Return TF whether the proxy definition is any good. TODO add other conditions'''
        return LDSUtilities.mightAsWellBeNone(pxy) and pxy.values()!=[':']
    
    @staticmethod
    def convertBool(sbool):
        '''Returns the bool representation of a T/F string or failing that whatever bool func thinks'''
        if isinstance(sbool,str) or isinstance(sbool,unicode):
            if sbool.lower() in ['true','t','yes','y']:
                return True
            elif sbool.lower() in ['false','f','no','n']:
                return False
        return bool(sbool)
    
    @staticmethod
    def mightAsWellBeNone(nstr):
        '''Doesn't cover all possibilities but accounts for most read-from-file (string) problems. Lists treated as ANY(None)->None'''
        #for when integers slip through and zeroes get represented as none
        if isinstance(nstr,int):
            ldslog.warn('Converting Integer {} to String for null comparison'.format(nstr))
            return str(nstr)
        if isinstance(nstr,tuple) or isinstance(nstr,list):
            return None if any(not LDSUtilities.mightAsWellBeNone(i) for i in nstr) else nstr
        elif isinstance(nstr,dict):
            #Case for dicts that have no valid values, may not be whats wanted
            return None if any(not LDSUtilities.mightAsWellBeNone(i) for i in nstr.values()) else nstr
        else:
            if isinstance(nstr,str) and (nstr == 'None' or nstr == '' or all(i in whitespace for i in nstr)):
                return None
        #if its already none this will return itself
        return nstr
    
    '''Enumeration method'''
    
    @staticmethod
    def enum(*sequential, **named):
        #http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
        enums = dict(zip(sequential, range(len(sequential))), **named)
        reverse = dict((value, key) for key, value in enums.iteritems())
        enums['reverse'] = reverse
        return type('Enum', (), enums)


    @staticmethod
    def setupLogging(lf=mainlog,ll=logging.DEBUG,ff=1):
        formats = {1:'%(asctime)s - %(levelname)s - %(module)s %(lineno)d - %(message)s',
                   2:':: %(module)s %(lineno)d - %(message)s',
                   3:'%(asctime)s,%(message)s'}
        
        log = logging.getLogger(lf)
        log.setLevel(ll)
        
        path = os.path.normpath(os.path.join(os.path.dirname(__file__), "../log/"))
        if not os.path.exists(path):
            os.mkdir(path)
        df = os.path.join(path,lf.lower()+'.log')
        
        fh = logging.FileHandler(df,'w')
        fh.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(formats[ff])
        fh.setFormatter(formatter)
        log.addHandler(fh)
        
        return log
    
        
    @staticmethod
    def standardiseLayerConfigName(layerprefix):
        '''Standardise to a layer config file name and check if it exists'''
        LP = '.layer.properties'
        layerprefix = LDSUtilities.standardiseDriverNames(layerprefix).lower()
        base = os.path.basename(layerprefix)
        filename = base + ('' if re.search(LP+'$', base) else LP)
        return os.path.abspath(os.path.join(os.path.dirname(__file__),'../conf/',filename))
    
    @classmethod
    def checkForLayerConfig(cls,layerprefix):
        '''Get standardised user config file name and check if it exists'''
        lpath = cls.standardiseLayerConfigName(layerprefix)
        return lpath if os.path.exists(lpath) else None
    
    @staticmethod
    def standardiseUserConfigName(userprefix):
        '''Standardise to a user config file name'''
        UP = '.conf'
        base = os.path.basename(str(userprefix))
        filename = base + ('' if re.search(UP+'$', base) else UP)
        return os.path.abspath(os.path.join(os.path.dirname(__file__),'../conf/',filename))
        
    @classmethod
    def checkForUserConfig(cls,userprefix):
        '''Get standardised user config file name and check if it exists'''
        upath = cls.standardiseUserConfigName(userprefix)
        return upath if os.path.exists(upath) else None

class FileResolver(etree.Resolver):
    def resolve(self, url, pubid, context):
        return self.resolve_filename(url, context)
    
class ConfigInitialiser(object):
    '''Initialises configuration, for use at first run'''

    @staticmethod
    def buildConfiguration(capsurl, wfs_ver,jorf, idp):
        '''Given a destination DS use this to select an XSL transform object and generate an output document that will initialise a new config file/table'''
        #file name subst for testing
        #capsurl='http://data.linz.govt.nz/services;key=<api-key>/wfs?service=WFS&version=2.0.0&request=GetCapabilities'
        #capsurl='http://data.linz.govt.nz/services;key=<api-key>/wfs?service=WFS&version=1.1.0&request=GetCapabilities'
        #xslfile='~/git/LDS/LDSReplicate/conf/getcapabilities-wfs2.0.json.xsl'
        #xslfile='~/git/LDS/LDSReplicate/conf/getcapabilities-wfs1.1.json.xsl'

        parser = etree.XMLParser(recover=True, huge_tree=True)
        parser.resolvers.add(FileResolver())
        
        wfspart = '-wfs{}'.format(wfs_ver)
        jorfpart = 'json' if jorf else 'file'
        xslfile = os.path.join(os.path.dirname(__file__), '../conf/getcapabilities{}.{}.xsl'.format(wfspart,jorfpart))
        
        xml = etree.parse(capsurl,parser)
        xsl = etree.parse(xslfile,parser)
        
        #this is a problem that seems to only affect eclipse, running from CL or the final bin is fine
        #FT = xml.findall('//{http://www.opengis.net/wfs/2.0}FeatureType')
        #KY = xml.findall('//{http://www.opengis.net/ows/1.1}Keywords/{http://www.opengis.net/ows/1.1}Keyword')
        #TX = xsl.findall('//{http://www.w3.org/1999/XSL/Transform}text')
        #print 'FT',len(FT)#,FT,[l.text for l in FT]
        #print 'KY',len(KY)#,KY,[l.text for l in KY]
        #print 'TX',len(TX)#,TX,[l.text for l in TX]
        
        transform = etree.XSLT(xsl)
        result = transform(xml,profile_run=True)
        ldslog.critical('RES>>>'+unicode(result)+'//'+str(result.xslt_profile))

        return (ConfigInitialiser._hackPrimaryKeyFieldJSON if jorf else ConfigInitialiser._hackPrimaryKeyFieldCP)(str(result),idp)
    
    @staticmethod 
    def cleanCP(cp):
        '''Make sure the ConfigParser is empty... even needed?'''
        for sec in cp.sections():
            cp.remove_section(sec)
        
    @staticmethod
    def _hackPrimaryKeyFieldCP(cpdoc,idp,csvfile=os.path.join(os.path.dirname(__file__),'../conf/ldspk.csv')):
        '''temporary hack method to rewrite the layerconf primary key field for ConfigParser file types using Koordinates supplied PK CSV'''
        import io
        from ConfigParser import ConfigParser, NoSectionError
        cp = ConfigParser()
        #read CP from GC doc
        cp.readfp(io.BytesIO(str(cpdoc)))

        #read the PK list writing any PK's found into CP
        for item in ConfigInitialiser.readCSV(csvfile):
            try:
                ky = item[2].replace('"','').lstrip()
                for lt in LDSUtilities.LORT:
                    ly = str(idp+lt+'-'+item[0])
                    if cp.has_section(ly):
                        cp.set(ly,'pkey',ky)
                        ldslog.debug('Setting PK on layer. '+ly+'//'+ky)
                        break
                else:
                    raise NoSectionError('No section matching '+idp+'|'.join(LDSUtilities.LORT)+'-'+item[0])
            except NoSectionError as nse:
                ldslog.warn('PK hack CP: '+str(nse)+ly+'//'+ky)

        #CP doesn't have a simple non-file write method?!?
        cps = "# LDS Layer Properties Initialiser - File\n"
        for section in cp.sections():
            ldslog.critical('writing >>>'+str(section))
            cps += "\n["+str(section)+"]\n"
            for option in cp.options(section):
                cps += str(option)+": "+str(cp.get(section, option))+"\n"

        return cps
    
    @staticmethod
    def _hackPrimaryKeyFieldJSON(jtext,idp,csvfile=os.path.join(os.path.dirname(__file__),'../conf/ldspk.csv')):
        '''temporary hack method to rewrite the layerconf primary key field in JSON responses'''
        import json
        
        jdata = json.loads(jtext)

        for item in ConfigInitialiser.readCSV(csvfile):
            for jline in jdata:
                if idp+item[0] == str(jline[0]):
                    jline[1] = item[2].replace('"','').lstrip()

                        
        return json.dumps(jdata)
    
    @staticmethod
    def readCSV(csvfile=os.path.join(os.path.dirname(__file__),'../conf/ldspk.csv')):
        '''Look for PK assigments in the Koordinates supplied csv'''
        import csv
        
        res = []
        with open(csvfile, 'rb') as csvtext:
            reader = csv.reader(csvtext, delimiter=',', quotechar='"')
            reader.next()
            for line in reader:
                res.append(line)
        return res
        
    @staticmethod
    def getConfFiles(confdir=os.path.join(os.path.dirname(__file__),'../conf/')):
        from lds.ReadConfig import MainFileReader as MF
        return sorted([f.split('.')[0] for f in os.listdir(confdir) if re.search('(?!^'+MF.DEFAULT_MF+'$)^.+\.conf$',f)]) 
        
        
    
class SUFIExtractor(object):
    '''XSL parser to read big int columns returning a dict of id<->col matches'''
    @staticmethod
    def readURI(xml,colname):
        p = os.path.join(os.path.dirname(__file__), '../conf/sufiselector.xsl')
        with open(p,'r') as sufireader:
            converter = sufireader.read()
        xslt = etree.XML(converter.replace('#REPLACE',colname))
        transform = etree.XSLT(xslt)
        
        doc = etree.parse(StringIO(xml))
        res = transform(doc)
        
        sufi = ast.literal_eval(str(res))
        #ldslog.debug(sufi)
        
        return sufi
    
class FeatureCounter(object):
    '''XSL parser to read big int columns returning a dict of id<->col matches'''
    @staticmethod
    def readCount(xml):
        p = os.path.join(os.path.dirname(__file__), '../conf/featurecounter.xsl')
        with open(p,'r') as featcount:
            converter = featcount.read()
        xslt = etree.XML(converter)
        transform = etree.XSLT(xslt)
        
        doc = etree.parse(StringIO(xml))
        res = transform(doc)
        
        fcval = ast.literal_eval(str(res))
        
        return fcval
    
class Encrypt(object):
    from lds.ReadConfig import MainFileReader
    ENC_PREFIX = "ENC:"
    #SbO, not secret at all actually
    p = LDSUtilities.standardiseUserConfigName(MainFileReader.DEFAULT_MF)
    with open(p,'r') as confile:
        lds = confile.readline(16)
    from Crypto import Random
    ivstr = Random.get_random_bytes(16)
    
    
    @classmethod
    def secure(cls,plaintext):
        import base64
        from Crypto.Cipher import AES
        aes = AES.new(cls.lds, AES.MODE_CBC, cls.ivstr)
        sec = base64.b64encode(aes.encrypt(Encrypt._pad(plaintext)))
        return sec

    
    @classmethod
    def unSecure(cls,sectext):
        import base64
        from Crypto.Cipher import AES
        aes = AES.new(cls.lds, AES.MODE_CBC, cls.ivstr)
        plain = Encrypt._strip(aes.decrypt(base64.b64decode(sectext)))
        return plain
    
    @staticmethod
    def _pad(sectext):
        import random
        pn = 15-len(sectext)%16
        pad = '' if pn==0 else str(random.randint(10**(pn-1),10**pn-1))
        return sectext+pad+hex(pn)[2:]#.lstrip('0x') doesn't work for 0x0
        
    @staticmethod    
    def _strip(padtext):
        pn = padtext[-1]
        return padtext[:len(padtext)-int(pn,16)-1]
        

#this is the only class that emits LCE objects and a a utility module we don't want to be adding dependencies so LCE belongs here        
class LayerConfEntry(object):
    '''Storage class for layer config info'''
    def __init__(self,id,pkey,name,group,gcol,epsg,lmod,disc,cql):
        self.id = id
        self.pkey = pkey
        self.name = name
        self.group = group
        self.gcol = gcol
        self.epsg = epsg
        self.lmod = lmod
        self.disc = disc
        self.cql = cql
        
    def __str__(self):
        return 'LCE {}={} - {}'.format(self.pkey if LDSUtilities.mightAsWellBeNone(self.pkey) else '_id', self.id, self.name)
    
        
