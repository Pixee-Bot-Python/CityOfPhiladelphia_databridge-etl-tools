import os
import sys
from time import sleep
import logging
import zipfile
import click
import petl as etl
from arcgis import GIS


class AGO():
    _logger = None
    _org = None
    _item = None
    _item_geom = None
    _item_fields = None

    def __init__(self,
                 ago_org_url,
                 ago_user,
                 ago_password,
                 item_name,
                 item_type,
                 **kwargs
                 ):
        self.ago_org_url = ago_org_url
        self.ago_user = ago_user
        self.ago_password = ago_password
        self.item_name = item_name
        self.item_type = item_type
        self.proxy_host = kwargs.get('proxy_host', None)
        self.proxy_port = kwargs.get('proxy_port', None)
        self.export_format = kwargs.get('export_format', None)
        self.export_zipped = kwargs.get('export_zipped', False)
        self.export_dir_path = kwargs.get('export_dir_path', os.getcwd() + '\\' + self.item_name.replace(' ', '_'))
        self.csv_path = kwargs.get('csv_path', None)


    @property
    def logger(self):
        if self._logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)
            sh = logging.StreamHandler(sys.stdout)
            logger.addHandler(sh)
            self._logger = logger
        return self._logger


    @property
    def org(self):
        if self._org is None:
            self.logger.info(f'Making connection to AGO account at {self.ago_org_url} with user {self.ago_user} ...')
            try:
                self._org = GIS(self.ago_org_url,
                                self.ago_user,
                                self.ago_password,
                                proxy_host=self.proxy_host,
                                proxy_port=self.proxy_port,
                                verify_cert=False)

                self.logger.info('Connected to AGO.\n')
            except Exception as e:
                self.logger.error(f'Failed making connection to AGO account at {self.ago_org_url} with user {self.ago_user} ...')
                raise e
        return self._org


    @property
    def item(self):
        if self._item is None:
            try:
                items = self.org.content.search(f'''owner:"{self.ago_user}" AND title:"{self.item_name}" AND type:"{self.item_type}"''')
                for item in items:
                    if item.title == self.item_name:
                        self._item = item
                        return self._item
            except Exception as e:
                self.logger.error(f'Failed searching for item owned by {self.ago_user} with title: {self.item_name} and type: {self.item_type}')
                raise e
        return self._item


    @property
    def item_tables(self):
        return self.item.tables

    @property
    def item_layers(self):
        return self.item.layers

    @property
    def item_geom(self):
        if self._item_geom is None:
            if self.item_tables and type(self.item_tables) is list:
                self._item_geom = 'False'
            elif self.item_layers and type(self.item_layers) is list:
                self._item_geom = 'True'
        return self._item_geom

    @property
    def item_fields(self):
        if self._item_fields is None:
            self._item_fields = self.item_tables[0].properties.fields if self.item_geom == 'False' else self.item_layers[0].properties.fields
        return self._item_fields


    def unzip(self):
        # get path to zipfile:
        zip_path = ''
        for root, subdirectories, files in os.walk(self.export_dir_path):
            for file in files:
                if '.zip' in file:
                    zip_path = os.path.join(root, file)
        # Unzip:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.export_dir_path)


    def export(self):
        # TODO: delete any existing files in export_dir_path
        # test parameters
        # parameters = {"layers" : [ { "id" : 0, "out_sr": 2272 } ] }
        # result = self.item.export(f'{self.item.title}', self.export_format, parameters=parameters, enforce_fld_vis=True, wait=True)
        result = self.item.export(f'{self.item.title}', self.export_format, enforce_fld_vis=True, wait=True)
        result.download(self.export_dir_path)
        # Delete the item after it downloads to save on space
        result.delete()
        # unzip, unless argument export_zipped = True
        if not self.export_zipped:
            self.unzip()


    def update(self):
        print(vars(self.item))
        if self.item_geom == 'True':
            from arcgis.features import FeatureLayerCollection
            flayer_collection = FeatureLayerCollection.fromitem(self.item)
            # call the overwrite() method which can be accessed using the manager property
            flayer_collection.manager.overwrite(self.csv_path)
        elif self.item_geom == 'False':
            table = self.item_tables[0]
            print("Truncating table")
            table.manager.truncate()  # truncate table
            count = table.query(return_count_only=True)
            print('count after truncate: ', count)
            assert count == 0
            rows = etl.fromcsv(self.csv_path, encoding='utf-8')
            row_dicts = rows.dicts()
            batch_size = 1000
            adds = []
            for i, row in enumerate(row_dicts):
                adds.append({"attributes": row})
                if len(adds) % batch_size == 0:
                    print("Adding batch...")
                    sleep(1)
                    table.edit_features(adds, rollback_on_failure=True)
                    adds = []
            if adds:
                table.edit_features(adds, rollback_on_failure=True)
            count = table.query(return_count_only=True)
            print('count after batch adds: ', count)
            # file_remove(self.csv_path)


    def append(self):
        print("Updating is not yet implemented...")
        raise NotImplementedError



@click.group()
def cli():
    pass

@cli.command('export')
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_password')
@click.option('--item_name')
@click.option('--item_type')
@click.option('--export_format',
              default="CSV",
              help='''The output format for the export. 
                    Values: Shapefile | CSV | File Geodatabase | Feature Collection | GeoJson | Scene Package | KML | Excel
                    ''')
def export_ago_item(ago_org_url, ago_user, ago_password, item_name, item_type, export_format=None):
    ago = AGO(ago_org_url,ago_user,ago_password,item_name,item_type,export_format=export_format)
    ago.export()


@cli.command('update')
@click.option('--ago_org_url')
@click.option('--ago_user')
@click.option('--ago_password')
@click.option('--item_name')
@click.option('--item_type')
@click.option('--csv_path',
              default="",
              help='''The path to the csv file with the data for updating the AGO item.''')
def update_ago_item(ago_org_url, ago_user, ago_password, item_name, item_type, csv_path=None):
    ago = AGO(ago_org_url, ago_user, ago_password, item_name, item_type, csv_path=csv_path)
    ago.update()
    # print("item fields: ", ago.item_fields)


if __name__ == '__main__':
    cli()