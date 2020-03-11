#!/usr/bin/env python

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys
import json
import re

@Configuration()
class MkJSONCommand(StreamingCommand):
    """ 

    ##Syntax


    ##Description


    ##Example


    """
    includehidden = Option(require=False, validate=validators.Boolean())
    outputfield = Option(require=False, validate=validators.Fieldname())
    sortkeys = Option(require=False, validate=validators.Boolean())
    parentobject = Option(require=False, validate=validators.Fieldname())

    def stream(self, events):

        if not self.outputfield:

            outputfield = "_raw"

        else:

            outputfield = self.outputfield

        if not self.includehidden:

            self.includehidden = False

        if not self.sortkeys:

            self.sortkeys = False

        for event in events:

            includedfields = set()

            if len(self.fieldnames) > 0:

                for fieldname in self.fieldnames:

                    if fieldname in event:

                        includedfields.add(fieldname)

                outputdict = {}

                for field in includedfields:

                    if len(event[field]) > 0:

                        outputdict[field] = event[field]
                        
                if self.parentobject:
                    
                    outputdict = {self.parentobject: outputdict}

                event[outputfield] = json.dumps(outputdict, sort_keys=self.sortkeys)

            else:

                outputdict = {}

                for field in event:

                    if self.includehidden or not re.match('^\_[^\_]',field):

                        if len(event[field]) > 0:

                            outputdict[field] = event[field]
                         
                if self.parentobject:
                    
                    outputdict = {self.parentobject: outputdict}
                    
                event[outputfield] = json.dumps(outputdict, sort_keys=self.sortkeys)

            yield event

dispatch(MkJSONCommand, sys.argv, sys.stdin, sys.stdout, __name__)
