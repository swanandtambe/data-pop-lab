import csv
import io
from logging import DEBUG, INFO

from nautobot.dcim.models import Device, Location, LocationType # type: ignore
from nautobot.apps.jobs import Job, register_jobs # type: ignore
from nautobot.extras.models import Status # type: ignore
from nautobot.extras.jobs import FileVar # type: ignore
from django.db import transaction # type: ignore

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
                a_status = Status.objects.get(name='Active')
            except Exception as e:
                self.logger.warning(f"No status found {e}")
            try:
                location_type_state_obj = LocationType.objects.get(name='State')
                location_state_obj, created = Location.objects.get_or_create(name=location_state,location_type=location_type_state_obj, status=a_status)
                if created:
                    self.logger.info(f'{location_state} State location created')
                location_state_obj.validated_save()
            except Exception as e:
                self.logger.info(f'Error while creating State location object row {row_num} {e}')
            try:
                location_type_city_obj = LocationType.objects.get(name='City')
                location_city_obj, created = Location.objects.get_or_create(name=location_city,location_type=location_type_city_obj, parent=location_state_obj, status=a_status)
                if created:
                    self.logger.info(f'{location_city} State location created')
                location_city_obj.validated_save()
            except Exception as e:
                self.logger.info(f'Error while creating City location object {e}')
            try:
                location_dc_br = location_name.split("-")[-1]
                if location_dc_br == 'DC':
                    location_type_obj = LocationType.objects.get(name='Data Center')
                    location_obj, created = Location.objects.get_or_create(name=location_name,location_type=location_type_obj, parent=location_city_obj, status=a_status)
                    if created:
                        self.logger.info(f'{location_name} DC location created')
                    location_obj.validated_save()
                elif location_dc_br == 'BR':
                    location_type_obj = LocationType.objects.get(name='Branch')
                    location_obj, created = Location.objects.get_or_create(name=location_name,location_type=location_type_obj, parent=location_city_obj, status=a_status)
                    if created:
                        self.logger.info(f'{location_name} DC location created')
                    location_obj.validated_save()
                else:
                    self.logger.warning(f'Error with location entry on row {row_num}')
            except Exception as e:
                self.logger.info(f'Error while creating DC or BR location {e}')

register_jobs(CustomCSVJob)
