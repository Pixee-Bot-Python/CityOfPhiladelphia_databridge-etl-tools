import os
import sys
import logging
import zipfile
import click
import petl as etl
import boto3
import botocore
import pyproj
import shapely.wkt
from shapely.ops import transform as shapely_transformer
from arcgis import GIS
from arcgis.features import FeatureLayerCollection


class AGO():
    _logger = None
    _org = None
    _item = None
    _geometric = None
    _item_fields = None
    _layer_object = None
    _ago_srid = None
    _projection = None
    _geometric = None
    _transformer = None

    def __init__(self,
                 ago_org_url,
                 ago_user,
                 ago_pw,
                 ago_item_name,
                 s3_bucket,
                 csv_s3_key,
                 in_srid,
                 **kwargs
                 ):
        self.ago_org_url = ago_org_url
        self.ago_user = ago_user
        self.ago_password = ago_pw
        self.item_name = ago_item_name
        self.s3_bucket = s3_bucket
        self.csv_s3_key = csv_s3_key
        self.in_srid = in_srid
        self.proxy_host = kwargs.get('proxy_host', None)
        self.proxy_port = kwargs.get('proxy_port', None)
        self.export_format = kwargs.get('export_format', None)
        self.export_zipped = kwargs.get('export_zipped', False)
        self.export_dir_path = kwargs.get('export_dir_path', os.getcwd() + '\\' + self.item_name.replace(' ', '_'))
        # unimportant since this will be run in AWS batch
        self.csv_path = '/home/worker/temp.csv'


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


    '''Find the AGO object that we can perform actions on, sends requests to it's AGS endpoint in AGO.
    Contains lots of attributes we'll need to access throughout this script.'''
    @property
    def item(self):
        if self._item is None:
            try:
                # "Feature Service" seems to pull up both spatial and table items in AGO
                items = self.org.content.search(f'''owner:"{self.ago_user}" AND title:"{self.item_name}" AND type:"Feature Service"''')
                for item in items:
                    if item.title == self.item_name:
                        self._item = item
                        return self._item
            except Exception as e:
                self.logger.error(f'Failed searching for item owned by {self.ago_user} with title: {self.item_name} and type:"Feature Service"')
                raise e
        return self._item


    '''Get the item object that we can operate on Can be in either "tables" or "layers"
    but either way operations on it are the same.'''
    @property
    def layer_object(self):
        if self._layer_object is None:
            self.logger.info(f'AGO item url and id: {self.item.url}, {self.item.id}')
            # Necessary to "get" our item after searching for it, as the returned
            # objects don't have equivalent attributes.
            feature_layer_item = self.org.content.get(self.item.id)
            if feature_layer_item.tables:
                if feature_layer_item.tables[0]:
                    self._layer_object = feature_layer_item.tables[0]
            elif feature_layer_item.layers:
                if feature_layer_item.layers[0]:
                    self._layer_object = feature_layer_item.layers[0]
            if self._layer_object is None:
                raise AssertionError('Could not locate our feature layer/table item in returned AGO object')
        return self._layer_object


    '''detect the SRID of the dataset in AGO, we'll need it for formatting the rows we'll upload to AGO.
    record both the standard SRID (latestwkid) and ESRI's made up on (wkid) into a tuple.
    so for example for our standard PA state plane one, latestWkid = 2272 and wkid = 102729
    We'll need both of these.'''
    @property
    def ago_srid(self):
        if self._ago_srid is None:
            # Don't ask why the SRID is all the way down here..
            assert self.layer_object.container.properties.initialExtent.spatialReference is not None
            self._ago_srid = (self.layer_object.container.properties.initialExtent.spatialReference['wkid'],self.layer_object.container.properties.initialExtent.spatialReference['latestWkid'])
        return self._ago_srid


    '''Fields of the dataset in AGO'''
    @property
    def item_fields(self):
        self._item_fields = layer_object.properties.fields
        return self._item_fields


    '''Boolean telling us whether the item is geometric or just a table?'''
    @property
    def geometric(self):
        if self._geometric is None:
            self.logger.info('Determining geometric?...')
            geometry_type = None
            try:
                # Note, initially wanted to use hasGeometryProperties but it seems like it doesn't
                # show up for point layers. geometryType is more reliable I think?
                #is_geometric = self.layer_object.properties.hasGeometryProperties
                geometry_type = self.layer_object.properties.geometryType
            except:
                self._geometric = False
            if geometry_type:
                self._geometric = True
                self.logger.info(f'Item detected as geometric, type: {geometry_type}\n')
            else:
                self.logger.info(f'Item is not geometric.\n')
        return self._geometric


    '''Decide if we need to project our shape field. If the SRID in AGO is set
    to what our source dataset is currently, we don't need to project.'''
    @property
    def projection(self):
        if self._projection is None:
            if self.in_srid == self.ago_srid[1]:
                self.logger.info(f'source SRID detected as same as AGO srid, not projecting. source: {self.in_srid}, ago: {self.ago_srid[1]}\n')
                self._projection = False
            else:
                self.logger.info(f'Shapes will be projected. source: {self.in_srid}, ago: {self.ago_srid[1]}\n')
                self._projection = True
        return self._projection


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


    '''
    Based off docs I believe this will only work with fgdbs or sd file
    or with non-spatial CSV files: https://developers.arcgis.com/python/sample-notebooks/overwriting-feature-layers
    '''
    def overwrite(self):
        if self.geometric == 'True':
            raise NotImplementedError('Overwrite with CSVs only works for non-spatial datasets (maybe?)')
        print(vars(self.item))
        flayer_collection = FeatureLayerCollection.fromitem(self.item)
        # call the overwrite() method which can be accessed using the manager property
        flayer_collection.manager.overwrite(self.csv_path)


    def truncate(self):
        self.layer_object.manager.truncate()
        count = self.layer_object.query(return_count_only=True)
        self.logger.info('count after truncate: ' + str(count))
        assert count == 0


    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.csv_s3_key))

        s3 = boto3.resource('s3')
        try:
            s3.Object(self.s3_bucket, self.csv_s3_key).download_file(self.csv_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise AssertionError(f'CSV file doesnt appear to exist in S3! key: {self.csv_s3_key}')
            else:
                raise e

        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.csv_s3_key))


    '''transformer needs to be defined outside of our row loop to speed up projections.'''
    @property
    def transformer(self):
        if self._transformer is None:
            self._transformer = pyproj.Transformer.from_crs(f'epsg:{self.in_srid}',
                                                      f'epsg:{self.ago_srid[1]}',
                                                      always_xy=True)
        return self._transformer


    ''' Helper function to help format spatial fields properly for AGO '''
    def project_and_format_shape(self, wkt_shape):
        # Note: list of coordinates for polygons are called "rings" for some reason
        def format_ring(poly):
            if self.projection:
                transformed = shapely_transformer(self.transformer.transform, poly)
                xlist = list(transformed.exterior.xy[0])
                ylist = list(transformed.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(poly.exterior.xy[0])
                ylist = list(poly.exterior.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        def format_path(line):
            if self.projection:
                transformed = shapely_transformer(self.transformer.transform, line)
                xlist = list(transformed.coords.xy[0])
                ylist = list(transformed.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
            else:
                xlist = list(line.coords.xy[0])
                ylist = list(line.coords.xy[1])
                coords = [list(x) for x in zip(xlist, ylist)]
                return coords
        if 'POINT' in wkt_shape:
            pt = shapely.wkt.loads(wkt_shape)
            if self.projection:
                x, y = self.transformer.transform(pt.x, pt.y)
                return x, y
            else:
                return pt.x, pt.y
        elif 'MULTIPOLYGON' in wkt_shape:
            multipoly = shapely.wkt.loads(wkt_shape)
            assert multipoly.is_valid
            list_of_rings = []
            for poly in multipoly:
                assert poly.is_valid
                # reference for polygon projection: https://gis.stackexchange.com/a/328642
                ring = format_ring(poly)
                list_of_rings.append(ring)
            return list_of_rings
        elif 'POLYGON' in wkt_shape:
            poly = shapely.wkt.loads(wkt_shape)
            assert poly.is_valid
            ring = format_ring(poly)
            return ring
        elif 'LINESTRING' in wkt_shape:
            path = shapely.wkt.loads(wkt_shape)
            path = format_path(path)
            return path
        else:
            raise NotImplementedError('Shape unrecognized.')


    ''' Do not perform project, simply extract and return our coords lists.'''
    def return_coords_only(self,wkt_shape):
        poly = shapely.wkt.loads(wkt_shape)
        return poly.exterior.xy[0], poly.exterior.xy[1]


    def append(self):
        rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        row_dicts = rows.dicts()
        batch_size = 5000
        adds = []
        if self.geometric is False:
            for i, row in enumerate(row_dicts):
                adds.append({"attributes": row})
                if len(adds) % batch_size == 0:
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {i}...')
                    self.layer_object.edit_features(adds, rollback_on_failure=True)
                    self.logger.info('Batch added.\n')
                    adds = []
            if adds:
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i}...')
                self.layer_object.edit_features(adds, rollback_on_failure=True)
                    self.logger.info('Batch added.\n')
        elif self.geometric is True:
            for i, row in enumerate(row_dicts):
                # remove the shape field so we can replace it with SHAPE with the spatial reference key
                # and also store in 'wkt' var (well known text) so we can project it
                wkt = row.pop('shape')
                # For different types we can consult this for the proper json format:
                # https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm
                if 'POINT' in wkt:
                    projected_x, projected_y = self.project_and_format_shape(wkt)
                                   # Format our row, following the docs on this one, see section "In [18]":
                    # https://developers.arcgis.com/python/sample-notebooks/updating-features-in-a-feature-layer/
                    # create our formatted point geometry
                    geom_dict = {"x": projected_x,
                                 "y": projected_y,
                                 "spatial_reference": {"wkid": 102100, "latestWkid": 3857}
                                 }
                    row_to_append = {"attributes": row,
                                     "geometry": geom_dict}
                elif 'MULTIPOINT' in wkt:
                    raise NotImplementedError("MULTIPOINTs not implemented yet..")
                elif 'MULTIPOLYGON' in wkt:
                    rings = self.project_and_format_shape(wkt)
                    geom_dict = {"rings": rings,
                                 "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                                 }
                    row_to_append = {"attributes": row,
                                     "geometry": geom_dict
                                     }
                elif 'POLYGON' in wkt:
                    #xlist, ylist = return_coords_only(wkt)
                    ring = self.project_and_format_shape(wkt)
                    geom_dict = {"rings": [ring],
                                 "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                                 }
                    row_to_append = {"attributes": row,
                                     "geometry": geom_dict
                                     }
                elif 'LINESTRING' in wkt:
                    paths = self.project_and_format_shape(wkt)
                    geom_dict = {"paths": [paths],
                                 "spatial_reference": {"wkid": self.ago_srid[0], "latestWkid": self.ago_srid[1]}
                                 }
                    row_to_append = {"attributes": row,
                                     "geometry": geom_dict
                                     }
                adds.append(row_to_append)

                batch_size = 1000
                # Where we actually append the rows to the dataset in AGO
                if len(adds) % batch_size == 0:
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {i}...')
                    self.logger.info(f'Example row: {adds[0]}')
                    self.layer_object.edit_features(adds=adds)
                    self.logger.info('Batch added.\n')
                    adds = []
            # add leftover rows outside the loop if they don't add up to 3000
            if adds:
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i}...')
                self.logger.info(f'Example row: {adds[0]}')
                self.layer_object.edit_features(adds=adds) 
                self.logger.info('Batch added.\n')


        count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(count)}')
        assert count != 0


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


@click.group()
def cli():
    pass

if __name__ == '__main__':
    cli()
