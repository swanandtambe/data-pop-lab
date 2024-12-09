import csv
import io
from logging import DEBUG, INFO

from nautobot.dcim.models import Device, Location
from nautobot.apps.jobs import Job, register_jobs
from nautobot.extras.jobs import FileVar
from django.db import transaction

class CustomCSVJob(Job):
    class Meta:
        """ Meta class for custom CSV job to import location
        
        Attributes:
            name (str): The name of the job.
            description (str): A description of the job's functionality.
        """
        name = "Import locations via CSV."
        description = "Reads data from CSV. And assigns locations."
        has_sensitive_variables = False

    csv_file = FileVar(required=True, description="Upload CSV. Performs a check of data.")

    @transaction.atomic
    def run(self, csv_file):
        """ Main function of the job """

        file_name = csv_file.name.split("/")[-1]
        self.logger.info(f"Reading CSV: {file_name}")

        if "locations" in file_name:
            self.logger.info("Matched file to locations: using 'name' as key")
            # Read CSV File and Output Overview of Rows.
            csv_file_content = csv_file.read().decode("utf-8")
            csv_reader = csv.DictReader(io.StringIO(csv_file_content))

        for row_num, data in enumerate(csv_reader, start=2):
            location_name = data.get("name")
            location_city = data.get("city")
            location_state = data.get("state")
            try:
                location_state_obj, created = Location.objects.get_or_create(name=location_state,location_type='State', status='Active')
                if created:
                    self.logger.info(f'{location_state} State location created')
                location_state_obj.validated_save()
            except:
                self.logger.info(f'Error while creating State location object row {row_num}')
            try:
                location_city_obj, created = Location.objects.get_or_create(name=location_city,location_type='City', parent=location_state_obj, status='Active')
                if created:
                    self.logger.info(f'{location_city} State location created')
                location_city_obj.validated_save()
            except:
                self.logger.info('Error while creating City location object')
            try:
                location_dc_br = location_name.split("-")[-1]
                if location_dc_br == 'DC':
                    location_obj, created = Location.objects.get_or_create(name=location_name,location_type='Data Center', parent=location_city_obj, status='Active')
                    if created:
                        self.logger.info(f'{location_name} DC location created')
                elif location_dc_br == 'BR':
                    location_obj, created = Location.objects.get_or_create(name=location_name,location_type='Branch', parent=location_city_obj, status='Active')
                    if created:
                        self.logger.info(f'{location_name} DC location created')
                else:
                    self.logger.warning(f'Error with location entry on row {row_num}')
            except:
                self.logger.info(f'Error while creating DC or BR location')


register_jobs(CustomCSVJob)
