#!/usr/bin/env python

from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
import sys
import json
import re

@Configuration(required_fields=["*"])
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

        if not self.includehidden:

            self.includehidden = False

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

                event[outputfield] = json.dumps(outputdict)

            else:

                outputdict = {}

                for field in event:

                    if self.includehidden or not re.match('^\_[^\_]',field):

                        if len(event[field]) > 0:

                            outputdict[field] = event[field]

                event[outputfield] = json.dumps(outputdict)

            yield event

dispatch(MkJSONCommand, sys.argv, sys.stdin, sys.stdout, __name__)
