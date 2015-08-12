'''
v.0.0.9

LDSReplicate -  RequestBuilder

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

URI builder subclasses for differing WFS implementations 

Created on 03/09/2013

@author: jramsay
'''
import re

from lds.DataStore import MalformedConnectionString, UnknownDSVersionException
from lds.LDSUtilities import LDSUtilities as LU, Debugging as DB

ldslog = LU.setupLogging()

URLLIST = {'LDSL1':'wfs.data.linz.govt.nz',
           'LDSL2':'data.linz.govt.nz',
           'LDST':'data-test.linz.govt.nz',
           'MFEL':'data.mfe.govt.nz'        
           }

'''
  +-----+-----+------+
  | wfs | gml | srs  |
  +-----+-----+------+
  | 1.0 | 2   | http |
  | 1.1 | 3   | urn  |
  | 2.0 | 3   | urn  |
  +-----+-----+------+
'''

class RequestBuilder(object):
    '''
    RB builds WFS urls for versions 100, 110 and 200.
    Usage:
    ver = 2.0.0
    p = (url,key,svc,ver,fmt,cql)
    rb = RequestBuilder.getInstance(p)
    u = rb.sourceURI()
    '''
    
    #LDS supports these formats but only GML is currently parsed
    SUPPORTED_OUTPUT_GML_FORMATS = ('GML2','GML3','JSON')
    DEFAULT_OUTPUT_GML_FORMAT = 'GML2'


    def __init__(self,params,conn_str=None):
        '''
        LDS init/constructor subclassing WFSDataStore
        '''
        self.setParameters(params)
        self.conn_str = conn_str
        #if conn_str provided get key from the string
        if self.conn_str:
            self.key = self.extractAPIKey(self.conn_str,raise_err=False)
            
        if not self.validateAPIKey(self.key):
            raise MalformedConnectionString('Invalid KEY, '+self.key)
        
    def __str__(self):
        return type(self).__name__
        
    def setParameters(self,params):
        (self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = params
        

            
    @staticmethod
    def getInstance(params,cs=None):
        '''Select RB based on WFS version as contained in params list'''
        version = params[3][:3]
        #print 'VERSION',version
        vf = {'1.0':RequestBuilderWFS110,'1.1':RequestBuilderWFS110,'2.0':RequestBuilderWFS200}
        if version in vf.keys():
            return vf[version](params,cs)
        else:
            raise UnknownDSVersionException('Cannot decode WFS version '+str(version))
        
    @staticmethod
    @DB.dres(prefix='1-2')
    def hitsAppend(url):
        '''Simple hits counter append but done here to capture resulting URL'''
        append = "&resultType=hits"
        return url+append
        
    def validateAPIKey(self,kstr):
        '''Make sure the provided key conforms to the required format'''
        srch = re.search('[a-f0-9]{32}',kstr,flags=re.IGNORECASE)
        if srch is None:
            raise MalformedConnectionString('Cannot parse API key, '+str(kstr))
        return True
        
    def extractAPIKey(self,cs,raise_err=False):
        '''if the user has supplied a connection string then they dont need to specify an API key in their config file, therefore we must extract it from the cs'''
        srch = re.search('/([a-f0-9]{32})/(v/x|wfs\?)',cs,flags=re.IGNORECASE)
        if srch is None and raise_err:
            raise MalformedConnectionString('Cannot parse API key')
        return srch.group(1) if srch else None
    
    def _buildCQLStr(self):
        '''Builds a cql_filter string as set by the user appending an 'id>...' partitioning string if needed. NB. Manual partitioning is accomplished using the parameters, 'maxFeatures' to set feature quantity, a page-by-page recorded 'id' value and a 'sortBy=id' argument'''
        cql = ()
        maxfeat = ""
        
#         #if implementing pagination in cql      
#         if self.pstart and self.psize:
#             cql += (self.pkey+">"+str(self.pstart),)
#             #sortBy used so last feature will have the new maximum key, saves a comparison
#             maxfeat = "&sortBy="+self.pkey+"&maxFeatures="+str(self.psize)            

        if self.cql:
            cql += (LU.checkCQL(self.cql),)

        return maxfeat+"&cql_filter="+';'.join(cql) if len(cql)>0 else ""   
    
    
    def validateConnStr(self,cs):
        '''WFS basic checks. 1 url format,2 api key,3 ask for wfs'''
        if not re.search('^http://',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('\'http\' declaration required in LDS request')
        #if not re.search('wfs\.data\.linz\.govt\.nz',cs,flags=re.IGNORECASE):
        #    raise MalformedConnectionString('Require \'wfs.data.linz.govt.nz\' in LDS address string')
        if not re.search('/[a-f0-9]{32}/(v/x|wfs\?)',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('Require API key (32char hex) in LDS address string')
        if not re.search('wfs\?',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('Need to specify \'wfs?\' service in LDS request')
        #look for conflicts
        ulayer = LU.getLayerNameFromURL(cs)

        return cs,ulayer
    
    
    @DB.dres(prefix='1-2')
    def sourceURIFeatureCount(self,layername):
        '''Endpoint constructor to fetch number of features for a specific layer. for: Trigger manual paging for broken JSON'''
        #version must be 1.1.0 or > for this to work. NB outputFormat doesn't seem to have any effect here either so its omitted
        typ = "&typeName="+layername
        return '{u}{k}/wfs?service={s}&version={v}&request=GetFeature&resultType=hits{t}'.format(u=self.url,k=self.key,s=self.svc,v=self.ver,t=typ)
    
    def getCapabilities(self):
        '''capabilities url cons'''
        #capabilities doc is fetched using urlopen, not wfs, so escaping isnt needed
        #uri = LU.xmlEscape(self.url+self.key+"/wfs?service=WFS&version=1.1.0&request=GetCapabilities")
        return '{u}services;key={k}/wfs?service={s}&version={v}&request=GetCapabilities'.format(u=self.url,k=self.key,s=self.svc,v=self.ver)
    
    def sourceURI(self,layername):
        '''Basic Endpoint constructor'''
        if not layername:
            ldslog.warn('No layer name provided to URI generator')
            return None
        
        if self.conn_str:
            valid,urilayer = self.validateConnStr(self.conn_str)
            if layername and urilayer!=layername:
                raise MalformedConnectionString('Layer specifications in URI differs from selected layer (-l); '+str(layername)+'!='+str(urilayer))
            return valid

        cql = self._buildCQLStr()
        #pql = self._buildPageStr()     
        typ = "##typeNames={}".format(layername)
        ver = "##version={}".format(self.ver) if self.ver else "##version={}".format(self.WVER)
        svc = "##service={}".format(self.svc) if self.svc else "##service=WFS"
        req = "##request=GetFeature"
        #if omitted the outputformat parameter is null and response is GML3.2.1 but this triggers surfaceMember Invalid errors
        fmt = "##outputFormat={}".format(self.fmt if (self.fmt in self.SUPPORTED_OUTPUT_GML_FORMATS) else self.DEFAULT_OUTPUT_GML_FORMAT)
        return re.sub('##','&',re.sub('##','?','{u}services;key={k}/wfs{s}{v}{r}{t}{f}{c}'.format(u=self.url,k=self.key,s=svc,v=ver,r=req,t=typ,f=fmt,c=cql),1))
        #return re.sub('##','&',re.sub('##','?','{u}{k}/wfs{s}{v}{r}{t}{c}'.format(u=self.url,k=self.key,s=svc,v=ver,r=req,t=typ,c=cql),1))
    

    def sourceURIIncremental(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific layers with incremental date fields'''
        if self.conn_str:
            valid,urilayer = self.validateConnStr(self.conn_str)
            #I don't know why you would attempt to specify dates in the CL and in the URL as well but we might as well attempt to catch diffs
            if layername and urilayer!=layername:
                raise MalformedConnectionString('Layer specifications in URI differs from selected layer (-l); '+str(layername)+'!='+str(urilayer))
            if (fromdate and re.search('from:'+fromdate[:10],valid) is None) or (todate and re.search('to:'+todate[:10],valid) is None):
                raise MalformedConnectionString("Date specifications in URI don't match those referred to with -t|-f "+str(todate)+'/'+str(fromdate)+" not in "+valid)
            return valid

        cql = self._buildCQLStr()
        #pql = self._buildPageStr()     
        
        vep = "{}-changeset".format(LU.splitLayerName(layername))
        typ = "##typeNames={}-changeset".format(layername)
        inc = "##viewparams=from:{};to:{}".format(fromdate,todate)
        ver = "##version={}".format(self.ver) if self.ver else "##version={}".format(self.WVER)
        svc = "##service={}".format(self.svc) if self.svc else "##service=WFS"
        req = "##request=GetFeature"
        #if omitted the outputformat parameter is null and response is GML3.2.1 but this triggers surfaceMember Invalid errors
        fmt = "##outputFormat={}".format(self.fmt if (self.fmt in self.SUPPORTED_OUTPUT_GML_FORMATS) else self.DEFAULT_OUTPUT_GML_FORMAT)
        return re.sub('##','&',re.sub('##','?','{u}services;key={k}{x}/wfs{s}{v}{r}{t}{i}{f}{c}'.format(u=self.url,k=self.key,x=vep,s=svc,v=ver,r=req,t=typ,i=inc,f=fmt,c=cql),1))
        #return re.sub('##','&',re.sub('##','?','{u}{k}{x}/wfs{s}{v}{r}{t}{i}{c}'.format(u=self.url,k=self.key,x=vep,s=svc,v=ver,r=req,t=typ,i=inc,c=cql),1))

class RequestBuilderWFS200(RequestBuilder):
    
    WVER = '2.0.0'
    
    def __init__(self,params,cs):
        super(RequestBuilderWFS200,self).__init__(params,cs)
        #HACK. below pulls the wfs prefix off urls implementing WFS2. We used re.sub since may apply to mfe urls as well
        #no way currently to tell if the url is hardcoded without a bit of a rewrite so user set url might get overwritten! Error message to announce
        self.url = LU.adjustWFS2URL(self.url,self.ver)
    
    @DB.dres(prefix=200)
    def getCapabilities(self):
        return super(RequestBuilderWFS200,self).getCapabilities()
    
    @DB.dres(prefix=200)
    def sourceURI(self,layername):
        return super(RequestBuilderWFS200,self).sourceURI(layername)
    
    @DB.dres(prefix=200)
    def sourceURIIncremental(self,layername,fromdate,todate):
        return super(RequestBuilderWFS200,self).sourceURIIncremental(layername,fromdate,todate)


class RequestBuilderWFS100(RequestBuilder):
    
    WVER = '1.0.0'
    
    @classmethod
    def new(cls,*args,**kwargs):
        return RequestBuilderWFS110(*args,**kwargs)

class RequestBuilderWFS110(RequestBuilder):
    
    WVER = '1.1.0'
    
    @DB.dres(prefix=110)
    def getCapabilities(self):
        return super(RequestBuilderWFS110,self).getCapabilities()
          
    @DB.dres(prefix=110)
    def sourceURI(self,layername):
        return super(RequestBuilderWFS110,self).sourceURI(layername)

    @DB.dres(prefix=110)        
    def sourceURIIncremental(self,layername,fromdate,todate):
        return super(RequestBuilderWFS110,self).sourceURIIncremental(layername,fromdate,todate)
      


    


        

        
        