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

    def stream(self, events):

        if not self.outputfield:

            outputfield = "_raw"

        else:

            outputfield = self.outputfield

        for event in events:

            includedfields = set()

            if len(self.fieldnames) > 0:

                for fieldname in self.fieldnames:

                    if fieldname in event:

                        includedfields.add(fieldname)

                outputdict = {}

                for field in includedfields:

                    outputdict[field] = event[field]

                event[outputfield] = json.dumps(outputdict)

            else:

                if self.includehidden:

                    if self.includehidden == True:

                        event[outputfield] = json.dumps(event)

                else:

                        outputdict = {}

                        for field in event:

                            if not re.match('^\_[^\_]',field):

                                outputdict[field] = event[field]

                        event[outputfield] = json.dumps(outputdict)

            yield event

dispatch(MkJSONCommand, sys.argv, sys.stdin, sys.stdout, __name__)
