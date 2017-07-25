"""
Command to create JSON for importing OpenMRS v1.11 sources into OpenMRS v2.02
Source file can be created as follows, for example:

    manage.py extract_source --raw -v0 --sources > sources.json

The 'raw' option indicates that JSON should be formatted one record per line (JSON lines file)
instead of human-readable format.

It is also possible to create a list of retired concept IDs (this is not used during import):

    manage.py extract_db --org_id=CIEL --source_id=CIEL --raw -v0 --retired > retired_concepts.json

You should validate reference sources before generating the export with the "check_sources" option:

    manage.py extract_db --check_sources --env=... --token=...

Set verbosity to 0 (e.g. '-v0') to suppress the results summary output. Set verbosity to 2
to see all debug output.

The OCL-CIEL test data set uses --concept_limit=2000:

    manage.py extract_db --org_id=CIEL --source_id=CIEL --raw -v0 --concept_limit=2000 --concepts > c2k.json
    manage.py extract_db --org_id=CIEL --source_id=CIEL --raw -v0 --concept_limit=2000 --mappings > m2k.json
    manage.py extract_db --org_id=CIEL --source_id=CIEL --raw -v0 --concept_limit=2000 --retired > r2k.json

NOTES:
- OCL does not handle the OpenMRS drug table -- it is ignored for now

BUGS:
- 'concept_limit' parameter simply uses the CIEL concept_id rather than the actual
   concept count, which means it only works for sequential numeric ID systems

"""
from optparse import make_option
import json

import datetime
from django.core.management import BaseCommand, CommandError
from omrs.models import Concept, ConceptReferenceSource
from omrs.management.commands import OclOpenmrsHelper, UnrecognizedSourceException
import requests



class Command(BaseCommand):
    """
    Extract concepts from OpenMRS database in the form of json
    """

    # Command attributes
    help = 'Extract concepts from OpenMRS database in the form of json'
    option_list = BaseCommand.option_list + (
        make_option('--raw',
                    action='store_true',
                    dest='raw',
                    default=False,
                    help='Format JSON for import, otherwise format for display.'),
        make_option('--sources',
                    action='store_true',
                    dest='source',
                    default=False,
                    help='Create Source Input File.'),
        make_option('--env',
                    action='store',
                    dest='ocl_api_env',
                    default='production',
                    help='Set the target for reference source validation to "dev", "staging", or "production"'),
        make_option('--token',
                    action='store',
                    dest='token',
                    default=None,
                    help='OCL API token to validate OpenMRS reference sources'),
    )

    OCL_API_URL = {
        'dev': 'http://api.dev.openconceptlab.com/',
        'staging': 'http://api.staging.openconceptlab.com/',
        'production': 'http://api.openconceptlab.com/',
    }



    ## EXTRACT_DB COMMAND LINE HANDLER AND VALIDATION

    def handle(self, *args, **options):
        """
        This method is called first directly from the command line, handles options, and calls
        either export() or retired_concept_id_export() depending on options set.
        """

        # Handle command line arguments
        self.raw = options['raw']
        self.do_source = options['source']
        self.verbosity = int(options['verbosity'])
        self.ocl_api_token = options['token']
        if options['ocl_api_env']:
            self.ocl_api_env = options['ocl_api_env'].lower()

        # Option debug output
        if self.verbosity >= 2:
            print 'COMMAND LINE OPTIONS:', options

        # Validate the options
        self.validate_options()

        # Determine if an export request
        self.do_export = False
        if self.do_source:
            self.do_export = True

        # Initialize counters
        self.cnt_sources_exported = 0
        self.cnt_total_sources_processed = 0

        # Process concepts, mappings, or retirement script
        if self.do_export:
            self.export()

        # Display final counts
        if self.verbosity:
            self.print_debug_summary()

    def validate_options(self):
        """
        Returns true if command line options are valid, false otherwise.
        Prints error message if invalid.
        """
        # If concept/mapping export enabled, org/source IDs are required & must be valid mnemonics
        # TODO: Check that org and source IDs are valid mnemonics
        # TODO: Check that specified org and source IDs exist in OCL
        if ( not self.do_source):
            raise CommandError(
                ("ERROR: source is an important parameter please pass it"))
        if self.ocl_api_env not in self.OCL_API_URL:
            raise CommandError('Invalid "env" option provided: %s' % self.ocl_api_env)
        return True

    def print_debug_summary(self):
        """ Outputs a summary of the results """
        print '------------------------------------------------------'
        print 'SUMMARY'
        print '------------------------------------------------------'
        print 'Total sources processed: %d' % self.cnt_total_sources_processed
        if self.do_source:
            print 'EXPORT COUNT: Concepts: %d' % self.cnt_sources_exported
        print '------------------------------------------------------'



    ## REFERENCE SOURCE VALIDATOR

    def check_sources(self):
        """ Validates that all reference sources in OpenMRS have been defined in OCL. """
        url_base = self.OCL_API_URL[self.ocl_api_env]
        headers = {'Authorization': 'Token %s' % self.ocl_api_token}
        reference_sources = ConceptReferenceSource.objects.all()
        reference_sources = reference_sources.filter(retired=0)
        enum_reference_sources = enumerate(reference_sources)
        for num, source in enum_reference_sources:
            source_id = OclOpenmrsHelper.get_ocl_source_id_from_omrs_id(source.name)
            if self.verbosity >= 1:
                print 'Checking source "%s"' % source_id

            # Check that source exists in the source directory (which maps sources to orgs)
            org_id = OclOpenmrsHelper.get_source_owner_id(ocl_source_id=source_id)
            if self.verbosity >= 1:
                print '...found owner "%s" in source directory' % org_id

            # Check that org:source exists in OCL
            if self.ocl_api_token:
                url = url_base + 'orgs/%s/sources/%s/' % (org_id, source_id)
                r = requests.head(url, headers=headers)
                if r.status_code != requests.codes.OK:
                    raise UnrecognizedSourceException('%s not found in OCL.' % url)
                if self.verbosity >= 1:
                    print '...found %s in OCL' % url
            elif self.verbosity >= 1:
                print '...no api token provided, skipping check on OCL.'

        return True



    ## MAIN EXPORT LOOP

    def export(self):
        """
        Main loop to export all concepts and/or their mappings.

        Loop thru all concepts and mappings and generates JSON export in the OCL format.
        Note that the retired status of concepts is not handled here.
        """

        # Set JSON indent value
        output_indent = 4
        if self.raw:
            output_indent = None

        source_results = ConceptReferenceSource.objects.all()
        source_enumerator = enumerate(source_results)


        # Iterate concept enumerator and process the export
        for num, source in source_enumerator:
            self.cnt_total_sources_processed += 1
            export_data = ''
            if self.do_source:
                export_data = self.export_source(source)
                if export_data:
                    print json.dumps(export_data, indent=output_indent)

        # self.print_debug_summary()



    ## CONCEPT EXPORT

    def export_source(self, source):
        """
        Export one concept as OCL-formatted dictionary.

        :param concept: Concept to export from OpenMRS database.
        :returns: OCL-formatted dictionary for the concept.

        Note:
        - OMRS does not have locale_preferred or description_type metadata, so these are omitted
        """

        # Iterate the concept export counter
        self.cnt_sources_exported += 1

        # Core concept fields
        # TODO: Confirm that all core concept fields are populated
        extras = {}
        data = {}
        data['concept_source_id'] = source.concept_source_id
        data['name'] = source.name
        data['external_id'] = source.uuid
        data['retired'] = source.retired
        extras['creator'] = source.creator
        data['date_created'] = self.datetime_handler(source.date_created)
        data['hl7_code'] = source.hl7_code
        data['description'] = source.description
        # TODO: Set additional concept extras
        data['extras'] = extras

        return data


    ## DATETIME HANDLER
    def datetime_handler(self,x):
        if isinstance(x,datetime.datetime):
            return x.isoformat()
        if x is None:
            return None
        return TypeError("Unknown Type")


## HELPER METHOD

def add_f(dictionary, key, value):
    """Utility function: Adds new field to the dictionary if value is not None"""
    if value is not None:
        dictionary[key] = value
