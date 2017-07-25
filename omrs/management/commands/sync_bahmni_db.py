"""
Command to using concept dictionary JSON files created from OpenMRS v1.11 concept dictionary into Bahmni.

Example usage:

    manage.py sync_bahmni_db --org_id=CIEL --source_id=CIEL

Set verbosity to 0 (e.g. '-v0') to suppress the results summary output. Set verbosity to 2
to see all debug output.

NOTES:
- Does not handle the OpenMRS drug table -- it is ignored for now

BUGS:

"""

import json
from optparse import make_option
from django.db.models import Max
from django.db.utils import IntegrityError
from django.db.models import ObjectDoesNotExist
import datetime
import uuid
import iso8601
import requests

from django.core.management import BaseCommand, CommandError
from omrs.management.commands import (OclOpenmrsHelper, UnrecognizedSourceException)
from omrs.models import ConceptReferenceSource, Concept, ConceptAnswer, ConceptClass, ConceptDatatype, ConceptName, ConceptReferenceMap, ConceptReferenceSource, ConceptSet, ConceptReferenceTerm, ConceptMapType, ConceptNumeric, ConceptDescription


class Command(BaseCommand):
    """
    Synchronize Bahmni/OpenMRS DB with concepts and mappping using OCL formatted json files
    """
    INTERNAL_MAP = 0
    EXTERNAL_MAP = 1

    # Command attributes
    help = 'Synchronize Bahmni/OpenMRS DB with concepts and mappping'
    option_list = BaseCommand.option_list + (
        make_option('--concept_file',
                    action='store',
                    dest='concept_filename',
                    default=None,
                    help='OCL concept filename'),
        make_option('--concept',
                    action='store_true',
                    dest='concept',
                    default=False,
                    help='Concept'),
        make_option('--mapping',
                    action='store_true',
                    dest='mapping',
                    default=False,
                    help='Mapping'),
        make_option('--keys',
                    action='store',
                    dest='keys',
                    default=None,
                    help='Keys File'),
        make_option('--mapping_file',
                    action='store',
                    dest='mapping_filename',
                    default=None,
                    help='OCL mapping filename'),
        make_option('--concept_id',
                    action='store',
                    dest='concept_id',
                    default=None,
                    help='ID for concept to sync, if specified only sync this one. e.g. 5839'),
        make_option('--retired',
                    action='store_true',
                    dest='retire_sw',
                    default=False,
                    help='If specify, output a list of retired concepts.'),
        make_option('--org_id',
                    action='store',
                    dest='org_id',
                    default=None,
                    help='org_id that owns the dictionary being imported (e.g. WHO)'),
        make_option('--source_id',
                    action='store',
                    dest='source_id',
                    default=None,
                    help='source_id of dictionary being imported (e.g. ICD-10-WHO)'),
        make_option('--check_sources',
                    action='store_true',
                    dest='check_sources',
                    default=False,
                    help='Validates that all reference sources in OpenMRS have been defined in OCL.'),
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
        either sync_db() or ??() depending on options set.
        """

        # Handle command line arguments
        self.org_id = options['org_id']
        self.concept=options['concept']
        self.mapping = options['mapping']
        if self.mapping:
            self.keys = options['keys']
            self.mapping_filename = options['mapping_filename']
        self.source_id = options['source_id']
        if self.concept:
            self.concept_id = options['concept_id']
            self.concept_filename = options['concept_filename']


        self.do_retire = options['retire_sw']

        self.verbosity = int(options['verbosity'])
        self.ocl_api_token = options['token']
        if options['ocl_api_env']:
            self.ocl_api_env = options['ocl_api_env'].lower()

        # Option debug output
        if self.verbosity >= 2:
            print 'COMMAND LINE OPTIONS:', options

        # Validate the options
        self.validate_options()

        # Load the concepts and mapping file into memory
        # NOTE: This will only work if it can fit into memory -- explore streaming partial loads




        if self.concept:
            self.concepts_id_added = {}
            concepts = []
            for line in open(self.concept_filename, 'r'):
                concepts.append(json.loads(line))
            self.sync_db(concepts=concepts)
        if self.mapping:
            mappings = []
            with open(self.keys, 'r') as fp:
                self.concepts_id_added = json.load(fp)
            for line in open(self.mapping_filename, 'r'):
                mappings.append(json.loads(line))
            self.sync_db(mappings=mappings)

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
        if (not self.concept and not self.mapping):
            raise CommandError(
                ("ERROR: concept and mapping  are required options "))
        if (self.mapping and not self.keys and not self.mapping_filename):
            raise CommandError(
                ("ERROR:  mapping json file and keys file names are required options "))
        if (self.concept and not self.concept_filename):
            raise CommandError(
                ("ERROR: concept  json file name is required option "))
        if self.ocl_api_env not in self.OCL_API_URL:
            raise CommandError('Invalid "env" option provided: %s' % self.ocl_api_env)
        return True

    def print_debug_summary(self):
        """ Outputs a summary of the results """
        print '------------------------------------------------------'
        print 'SUMMARY'
        print '------------------------------------------------------'
        print 'Total concepts processed: %d' % self.cnt_total_concepts_processed
        if self.do_concept:
            print 'EXPORT COUNT: Concepts: %d' % self.cnt_concepts_exported
        if self.do_mapping:
            print 'EXPORT COUNT: All Mappings: %d' % (self.cnt_internal_mappings_exported +
                                                      self.cnt_external_mappings_exported +
                                                      self.cnt_answers_exported +
                                                      self.cnt_set_members_exported)
            print 'EXPORT COUNT: Internal Mappings: %d' % self.cnt_internal_mappings_exported
            print 'EXPORT COUNT: External Mappings: %d' % self.cnt_external_mappings_exported
            print 'EXPORT COUNT: Linked Answer Mappings: %d' % self.cnt_answers_exported
            print 'EXPORT COUNT: Set Member Mappings: %d' % self.cnt_concepts_exported
            print 'Questions Processed: %d' % self.cnt_questions_exported
            print 'Concept Sets Processed: %d' % self.cnt_concept_sets_exported
            print 'Ignored Self Mappings: %d' % self.cnt_ignored_self_mappings
        if self.do_retire:
            print 'EXPORT COUNT: Retired Concept IDs: %d' % self.cnt_retired_concepts_exported
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

    def sync_db(self, concepts=None, mappings=None):
        """
        Main loop to sync all concepts and/or their mappings.

        Loop thru all concepts and mappings and generates needed entries.
        Note that the retired status of concepts is not handled here.
        """
        output_indent = None
        # Create the concept enumerator, applying 'concept_id'
        if self.concept:
            if self.concept_id is not None:
                # If 'concept_id' option set, fetch a single concept and convert to enumerator
                for i in range(len(concepts)):
                    if concepts[i]['id'] == int(self.concept_id):
                        concept_enumerator = enumerate([concepts[i]])
                        break


            else:
                # Fetch all concepts
                concept_enumerator = enumerate(concepts)
            # Iterate concept enumerator and process the export
            for num, concept in concept_enumerator:
                self.sync_concept(concept)

            data = self.concepts_id_added
            with open('/home/rishabh/Developer/ccbd_internship/OCL/omrs/keys_new.json', 'w') as fp:
                json.dump(data, fp)

        if self.mapping:
            external_mapping = self.generate_external_mapping(mappings)
            internal_mapping = self.generate_internal_mapping(mappings)
            self.sync_external_mapping(external_mapping)
            self.sync_internal_mapping(internal_mapping)

    def sync_concept(self, concept):
        """
        Create one concept and its mappings.

        :param concept: Concept to write to OpenMRS database and list of mappings.
        :returns: None.

        Note:
        - OMRS does not have locale_preferred or description_type metadata, so these are omitted
        """


        # Concept class, check if it is already created
        con_id = concept['id']

        if con_id:

            #Check Concept Class

            conc_class = ConceptClass.objects.filter(name=concept['concept_class'])
            concept_class = None
            if len(conc_class) != 0:
                concept_class = conc_class[0]
            else:
                uuidcc = uuid.uuid1()
                concept_class = ConceptClass(name=concept['concept_class'], retired=concept['retired'],
                                             creator=1, date_created=datetime.datetime.now(), uuid=uuidcc)
                concept_class.save()
                conc_class = ConceptClass.objects.filter(name=concept['concept_class'])
                concept_class = conc_class[0]


            #Obtain datatype ID from concept_datatype
            datatypes = ConceptDatatype.objects.filter(name=concept['datatype'])
            datatype = None
            if len(datatypes) != 0:
                datatype = datatypes[0]
            else:
                datatype = ConceptDatatype(name=concept['datatype'], creator=1, date_created=datetime.datetime.now())
                datatype.save()
                datatypes = ConceptDatatype.objects.filter(name=concept['datatype'])
                datatype = datatypes[0]


            f_sp = 0
            at_lst_one = 0

            cnames = concept['names']

            concept['is_set'] = 0
            if 'is_set' in concept['extras']:
                concept['is_set'] = concept['extras']['is_set']

            for cname in cnames:
                    concept_name = ConceptName.objects.filter(name=cname['name'], concept_name_type=cname['name_type'],
                                                              locale=cname['locale'], locale_preferred=cname['locale_preferred'])
                    if len(concept_name) != 0:
                        at_lst_one = 1 # at least one concept present
                        if len(concept_name) > 1:
                            for a in concept_name:
                                if a.concept_name_type == 'FULLY_SPECIFIED':
                                    f_sp = 1
                                    con_id = a.concept_id

                            if not f_sp:
                                concept_name = concept_name[0]
                                con_id = concept_name.concept_id
                            #print(id)
                        else:
                            if not f_sp:
                                concept_name = concept_name[0]
                                if concept_name.concept_name_type == 'FULLY_SPECIFIED':
                                    f_sp = 1
                                #concept_name = ConceptName.objects.get(name=cname['name'],concept_name_type=cname['name_type'],locale=cname['locale'],locale_preferred=cname['locale_preferred'])
                                con_id = concept_name.concept_id
                                #print(cname['name'])
                                #print(id)

                        self.concepts_id_added[concept['id']] = con_id
            if at_lst_one == 0:
                #all concept names have to be inserted
                conc = Concept.objects.filter(concept_id=con_id)
                if len(conc) != 0:# that id exists
                    #generate new id that is not in openmrs
                    cconc = Concept.objects.aggregate(Max('concept_id'))
                    con_id = cconc['concept_id__max']+1

                conc = Concept(concept_id=con_id, retired=concept['retired'], datatype=datatype, creator = 1,
                               date_created = datetime.datetime.now(),
                               concept_class=concept_class, uuid=concept['external_id'], is_set=concept['is_set'])
                conc.save()
                self.concepts_id_added[concept['id']] = con_id
            #print id
            for cname in cnames:
                concept_name = ConceptName.objects.filter(name=cname['name'], concept_name_type=cname['name_type'],
                                                          locale=cname['locale'], locale_preferred=cname['locale_preferred'])
                if len(concept_name) == 0:#if concept name not there
                    conc = Concept.objects.get(concept_id=con_id)
                    concept_name = ConceptName(concept=conc, name=cname['name'], uuid=cname['external_id'],
                                               creator = 1, date_created = datetime.datetime.now(),
                                               concept_name_type=cname['name_type'], locale=cname['locale'],
                                               locale_preferred=cname['locale_preferred'], voided=cname['voided'])
                    concept_name.save()
            conc = Concept.objects.get(concept_id=con_id)
            # Concept Descriptions

            for cdescription in concept['descriptions']:
                concept_description = ConceptDescription.objects.filter(concept=conc,
                                                                        description=cdescription['description'],
                                                                        uuid=cdescription['external_id'])
                if len(concept_description) == 0:
                    concept_description = ConceptDescription(concept=conc,
                                                             description=cdescription['description'],
                                                             uuid=cdescription['external_id'],
                                                             locale=cdescription['locale'], creator=1,
                                                             date_created=datetime.datetime.now())
                    concept_description.save()

            extra = None
            if concept['datatype'] == "Numeric":
                extra = concept['extras']
            # If the concept is of numeric type, map concept's numeric type data as extras
            if extra is not None:

                numeric = ConceptNumeric.objects.filter(concept=con_id)
                if len(numeric) == 0:
                    h_c = None
                    l_c = None
                    h_n = None
                    l_n = None
                    h_a = None
                    l_a = None
                    if 'hi_critical' in extra:
                        h_c = extra['hi_critical']
                    if 'low_critical' in extra:
                        l_c = extra['low_critical']
                    if 'hi_normal' in extra:
                        h_n = extra['hi_normal']
                    if 'low_normal' in extra:
                        l_n = extra['low_normal']
                    if 'hi_absolute' in extra:
                        h_a = extra['hi_absolute']
                    if 'low_absolute' in extra:
                        l_a = extra['low_absolute']
                    numeric = ConceptNumeric(concept=conc, hi_absolute=h_a,
                                             hi_critical=h_c, hi_normal=h_n, low_critical=l_c,
                                             low_absolute=l_a, low_normal=l_n,
                                             units=extra['units'], precise=extra['precise'])
                    numeric.save()





    ## CONCEPT and MAPPINGS sync to DB




    def segregate_mapping(self, mappings):
        mapping = []
        if self.concept_id is not None:
            for i in mappings:
                con_id = int(i['from_concept_url'].split('/')[-2])
                if con_id == int(self.concept_id):
                    mapping.append(i)
        return mapping


    def generate_id(self, concept=False, concept_set=False):
        if concept:
            return Concept.objects.all().aggregate(Max('concept_id'))['concept_id__max'] + 1
        elif concept_set:
            return ConceptSet.objects.all().aggregate(Max('concept_set_id'))['concept_set_id__max'] + 1
        return -1




    def generate_internal_mapping(self, mappings):
        s = "to_concept_url"
        internal_mapping = []
        for i in mappings:
            if s in i.keys():
                internal_mapping.append(i)
        return internal_mapping

    def generate_external_mapping(self, mappings):
        s = "to_source_url"
        external_mapping = []
        for i in mappings:
            if s in i.keys():
                external_mapping.append(i)
        return external_mapping

    def sync_internal_mapping(self, internal_mapping):
        for i in internal_mapping:
            if i['map_type'] == OclOpenmrsHelper.MAP_TYPE_CONCEPT_SET:
                from_concept_url = i["from_concept_url"] # Concept Set Owner
                to_concept_url = i['to_concept_url'] # Concept Set Member
                list_concept_url = from_concept_url.split("/")
                list_to_concept_url = to_concept_url.split("/")
                con_id = int(list_concept_url[-2])
                to_con_id = int(list_to_concept_url[-2])
                new_con_id = int(self.concepts_id_added[str(con_id)])
                new_to_con_id = int(self.concepts_id_added[str(to_con_id)])
                concept_own = Concept.objects.get(concept_id=new_con_id)
                concept_mem = Concept.objects.get(concept_id=new_to_con_id)
                mapping = ConceptSet.objects.filter(concept_set_owner=concept_own, concept=concept_mem)
                if len(mapping) == 0:
                    new_set_id = ConceptSet.objects.all().aggregate(Max('concept_set_id'))[
                                      'concept_set_id__max'] + 1
                    concept_set_to_save = ConceptSet(concept_set_id=new_set_id, concept=concept_mem,
                                                     concept_set_owner=concept_own,
                                                     creator=i['creator'],
                                                     date_created=datetime.datetime.now(),
                                                     uuid=str(uuid.uuid4()))
                    concept_set_to_save.save()
                # else:
                #
                #     from_concept = mapping[0].concept_set_owner
                #     to_concept = mapping[0].concept_id
                #     if not (new_con_id == from_concept and new_to_con_id == to_concept):
                #         new_id = self.generate_id(concept_set=True)
                #         concept = Concept.objects.filter(concept_id=new_con_id)
                #         concept_to = Concept.objects.filter(concept_id=new_to_con_id)
                #         concept_set_to_save = ConceptSet(concept_set_id=new_id, concept=concept[0], concept_set_owner=concept_to[0],
                #                                          creator=i['creator'], date_created=iso8601.parse_date(i['date_created']),
                #                                          uuid=i['external_id'])
                #         concept_set_to_save.save()
            elif i['map_type'] == OclOpenmrsHelper.MAP_TYPE_Q_AND_A:
                from_concept_url = i["from_concept_url"] # concept_id
                to_concept_url = i['to_concept_url']    #concept_answer
                list_concept_url = from_concept_url.split("/")
                list_to_concept_url = to_concept_url.split("/")
                con_id = int(list_concept_url[-2])
                to_con_id = int(list_to_concept_url[-2])
                new_con_id = int(self.concepts_id_added[str(con_id)])
                new_to_con_id = int(self.concepts_id_added[str(to_con_id)])
                concept_from = Concept.objects.get(concept_id=new_con_id)
                concept_to = Concept.objects.get(concept_id=new_to_con_id)
                mapping = ConceptAnswer.objects.filter(question_concept=concept_from, answer_concept=concept_to)
                if len(mapping) == 0:
                    new_ans_id = ConceptAnswer.objects.all().aggregate(Max('concept_answer_id'))['concept_answer_id__max'] + 1
                    concept_answer_to_save = ConceptAnswer(concept_answer_id=new_ans_id, question_concept=concept_from,
                                                           answer_concept=concept_to, creator=i['creator'], uuid=str(uuid.uuid4()),
                                                           date_created=datetime.datetime.now())
                    concept_answer_to_save.save()
            else:
                from_concept_url = i["from_concept_url"]
                to_concept_url = i['to_concept_url']
                list_concept_url = from_concept_url.split("/")
                list_to_concept_url = to_concept_url.split("/")
                con_id = int(list_concept_url[-2])
                to_con_id = int(list_to_concept_url[-2])
                new_con_id = int(self.concepts_id_added[str(con_id)])
                new_to_con_id = int(self.concepts_id_added[str(to_con_id)])
                concept_map_type = ConceptMapType.objects.filter(name=i['map_type'])
                concept = Concept.objects.get(concept_id=new_con_id)

                src = list_concept_url[-4]
                omrs_source_id = OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(ocl_source_id=src)
                source = ConceptReferenceSource.objects.get(name=omrs_source_id)
                try:
                    term = ConceptReferenceTerm.objects.get(code=str(to_con_id), concept_source=source)
                except ObjectDoesNotExist:
                    new_term_id = ConceptReferenceTerm.objects.all().aggregate(Max('concept_reference_term_id'))[
                                      'concept_reference_term_id__max'] + 1
                    term = ConceptReferenceTerm(concept_reference_term_id=new_term_id, concept_source=source,
                                                code=str(to_con_id),
                                                creator=i['creator'], date_created=datetime.datetime.now(), retired=i['retired'],
                                                uuid=str(uuid.uuid4()))
                    term.save()
                    term = ConceptReferenceTerm.objects.get(code=str(to_con_id), concept_source=source)
                mapping = ConceptReferenceMap.objects.filter(concept_reference_term=term, concept_id=concept,
                                                             map_type=concept_map_type[0])
                if len(mapping) == 0:
                        new_map_id = ConceptReferenceMap.objects.all().aggregate(Max('concept_map_id'))[
                                      'concept_map_id__max'] + 1
                        concept_map_to_save = ConceptReferenceMap(concept_map_id=new_map_id, creator=i['creator'],
                                                                  date_created=datetime.datetime.now(), concept=concept, uuid=str(uuid.uuid4()),
                                                                  concept_reference_term=term, map_type=concept_map_type[0])
                        concept_map_to_save.save()

    def sync_external_mapping(self, external_mapping):
        for i in external_mapping:
            from_concept_url = i["from_concept_url"]
            to_source_url = i['to_source_url']
            to_concept_code = i['to_concept_code']
            list_concept_url = from_concept_url.split("/")
            list_source_url = to_source_url.split("/")
            con_id = int(list_concept_url[-2])
            source_id = list_source_url[-2]
            omrs_source_id = OclOpenmrsHelper.get_omrs_source_id_from_ocl_id(ocl_source_id=source_id)
            source = ConceptReferenceSource.objects.filter(name=omrs_source_id)
            new_con_id = int(self.concepts_id_added[str(con_id)])
            concept = Concept.objects.filter(concept_id=new_con_id)
            concept_map_type = ConceptMapType.objects.filter(name=i['map_type'])
            try:
                term = ConceptReferenceTerm.objects.get(code=i['to_concept_code'], concept_source=source)
            except ObjectDoesNotExist:
                new_term_id = ConceptReferenceTerm.objects.all().aggregate(Max('concept_reference_term_id'))['concept_reference_term_id__max'] + 1
                term = ConceptReferenceTerm(concept_reference_term_id=new_term_id, concept_source=source[0],
                                                    code=to_concept_code,
                                                    creator=i['creator'], date_created=datetime.datetime.now(), retired=i['retired'],
                                                    uuid=str(uuid.uuid4()))
                term.save()
                term = ConceptReferenceTerm.objects.get(code=i['to_concept_code'], concept_source=source)
            mapping = ConceptReferenceMap.objects.filter(concept_reference_term=term, concept_id=concept[0],
                                                         map_type=concept_map_type[0])
            if len(mapping) == 0:
                concept_map = ConceptReferenceMap(concept_map_id=i['concept_map_id'], creator=i['creator'],
                                                  date_created=datetime.datetime.now(),
                                                  concept=concept[0], uuid=str(uuid.uuid4()),
                                                  concept_reference_term=term, map_type=concept_map_type[0])
                concept_map.save()
