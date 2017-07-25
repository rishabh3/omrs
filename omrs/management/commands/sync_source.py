"""
Command to sync the source and all the reference term in bahmni openmrs .
python manage.py --raw -v0 --source_file=source.json --term_file=term.json
This will read the source and reference term file and for a particular source it will check if source exists and if it doesnot exists then it
creates that particular source.
Also it collects all the terms that are related to the source we are working on and then checks if it exists on the basis of code because the
concept_reference_term_id may  not be unique but the code to which the concept is mapped in that source will be unique. Hence we can query for
reference term on the basis of code and then on running it syncs the database.
Another example to just sync one particular id of the source and all the terms is:
python manage.py --raw -v0 --source_file=source.json --term_file=term.json --source_id=1
"""
import uuid
from optparse import make_option
import json

import datetime

from django.core.management import BaseCommand, CommandError
from omrs.models import Concept, ConceptReferenceSource, ConceptReferenceTerm
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
        make_option('--source_file',
                    action='store',
                    dest='source_file',
                    default=None,
                    help='Source File Name'),
        make_option('--source_id',
                    action='store',
                    dest='source_id',
                    default=None,
                    help='source id'),
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
        self.source_file = options['source_file']
        self.source_id = options['source_id']
        self.verbosity = int(options['verbosity'])
        self.ocl_api_token = options['token']
        if options['ocl_api_env']:
            self.ocl_api_env = options['ocl_api_env'].lower()

        # Option debug output
        if self.verbosity >= 2:
            print 'COMMAND LINE OPTIONS:', options

        # Validate the options
        self.validate_options()

        # Initialize counters
        self.cnt_sources_exported = 0
        self.cnt_total_sources_processed = 0

        sources = []

        for line in open(self.source_file, 'r'):
            sources.append(json.loads(line))


        self.sync_source(sources)

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
        if (self.source_file is None):
            raise CommandError(
                ("ERROR: source file is an important parameter please pass it"))
        if self.ocl_api_env not in self.OCL_API_URL:
            raise CommandError('Invalid "env" option provided: %s' % self.ocl_api_env)
        return True

    def print_debug_summary(self):
        """ Outputs a summary of the results """
        print '------------------------------------------------------'
        print 'SUMMARY'
        print '------------------------------------------------------'
        print 'Total sources processed: %d' % self.cnt_total_sources_processed
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


    def sync_source(self,sources):
        if self.source_id is not None:
            for i in range(len(sources)):
                if sources[i]['concept_source_id'] == int(self.source_id):
                    source_enumerator = enumerate([sources[i]])
                    break

        else:
            # Fetch all concepts
            source_enumerator = enumerate(sources)

        # Iterate concept enumerator and process the export
        for num, source in source_enumerator:
            self.cnt_total_sources_processed += 1
            self.sync_source_db(source)

    def sync_source_db(self, source):

        source_present = ConceptReferenceSource.objects.filter(name=source['name'])
        if len(source_present) == 0:
            source_to_save = ConceptReferenceSource(name=source['name'], description=source['description'],
                                                    hl7_code=source['hl7_code'], date_created=datetime.datetime.now(), creator=source['extras']['creator'],
                                                    retired=source['retired'], uuid=str(uuid.uuid4()))
            source_to_save.save()





## HELPER METHOD

def add_f(dictionary, key, value):
    """Utility function: Adds new field to the dictionary if value is not None"""
    if value is not None:
        dictionary[key] = value
