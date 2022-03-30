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
import numpy as np
from pprint import pprint
from threading import Thread
from shapely.ops import transform as shapely_transformer
from arcgis import GIS
from arcgis.features import FeatureLayerCollection
from time import sleep, time


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
                 **kwargs
                 ):
        self.ago_org_url = ago_org_url
        self.ago_user = ago_user
        self.ago_password = ago_pw
        self.item_name = ago_item_name
        self.s3_bucket = s3_bucket
        self.csv_s3_key = csv_s3_key
        self.in_srid = None
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
                if self.proxy_host is None:
                    self._org = GIS(self.ago_org_url,
                                    self.ago_user,
                                    self.ago_password,
                                    verify_cert=False)
                else:
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
        '''Find the AGO object that we can perform actions on, sends requests to it's AGS endpoint in AGO.
        Contains lots of attributes we'll need to access throughout this script.'''
        if self._item is None:
            try:
                # "Feature Service" seems to pull up both spatial and table items in AGO
                items = self.org.content.search(f'''owner:"{self.ago_user}" AND title:"{self.item_name}" AND type:"Feature Service"''')
                for item in items:
                    if item.title == self.item_name:
                        self._item = item
                        self.logger.info(f'Found item, url and id: {self.item.url}, {self.item.id}')
                        return self._item
            except Exception as e:
                self.logger.error(f'Failed searching for item owned by {self.ago_user} with title: {self.item_name} and type:"Feature Service"')
                raise e
        return self._item


    @property
    def layer_object(self):
        '''Get the item object that we can operate on Can be in either "tables" or "layers"
        but either way operations on it are the same.'''
        if self._layer_object is None:
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


    @property
    def ago_srid(self):
        '''detect the SRID of the dataset in AGO, we'll need it for formatting the rows we'll upload to AGO.
        record both the standard SRID (latestwkid) and ESRI's made up on (wkid) into a tuple.
        so for example for our standard PA state plane one, latestWkid = 2272 and wkid = 102729
        We'll need both of these.'''
        if self._ago_srid is None:
            # Don't ask why the SRID is all the way down here..
            assert self.layer_object.container.properties.initialExtent.spatialReference is not None
            self._ago_srid = (self.layer_object.container.properties.initialExtent.spatialReference['wkid'],self.layer_object.container.properties.initialExtent.spatialReference['latestWkid'])
        return self._ago_srid


    @property
    def item_fields(self):
        '''Fields of the dataset in AGO'''
        self._item_fields = layer_object.properties.fields
        return self._item_fields


    @property
    def geometric(self):
        '''Boolean telling us whether the item is geometric or just a table?'''
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


    @property
    def projection(self):
        '''Decide if we need to project our shape field. If the SRID in AGO is set
        to what our source dataset is currently, we don't need to project.'''
        if self._projection is None:
            if str(self.in_srid) == str(self.ago_srid[1]):
                self.logger.info(f'source SRID detected as same as AGO srid, not projecting. source: {self.in_srid}, ago: {self.ago_srid[1]}\n')
                self._projection = False
            else:
                self.logger.info(f'Shapes will be projected. source: "{self.in_srid}", ago: "{self.ago_srid[1]}"\n')
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


    def overwrite(self):
        '''
        Based off docs I believe this will only work with fgdbs or sd file
        or with non-spatial CSV files: https://developers.arcgis.com/python/sample-notebooks/overwriting-feature-layers
        '''
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


    @property
    def transformer(self):
        '''transformer needs to be defined outside of our row loop to speed up projections.'''
        if self._transformer is None:
            self._transformer = pyproj.Transformer.from_crs(f'epsg:{self.in_srid}',
                                                      f'epsg:{self.ago_srid[1]}',
                                                      always_xy=True)
        return self._transformer


    def project_and_format_shape(self, wkt_shape):
        ''' Helper function to help format spatial fields properly for AGO '''
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


    def return_coords_only(self,wkt_shape):
        ''' Do not perform project, simply extract and return our coords lists.'''
        poly = shapely.wkt.loads(wkt_shape)
        return poly.exterior.xy[0], poly.exterior.xy[1]


    def append(self):
        rows = etl.fromcsv(self.csv_path, encoding='latin-1')
        self._num_rows_in_upload_file = rows.nrows()
        row_dicts = rows.dicts()
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
                if 'SRID=' not in wkt:
                    raise AssertionError("SRID not found in shape row! Please export your dataset with 'geom_with_srid=True'.")
                if self.in_srid == None:
                    self.in_srid = wkt.split(';')[0].strip("SRID=")
                wkt = wkt.split(';')[1]
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

                batch_size = 10000
                if len(adds) % batch_size == 0:
                    self.logger.info(f'Adding batch of {len(adds)}, at row #: {i+1}...')
                    start = time()
                    split_batches = np.array_split(adds,10)
                    # Where we actually append the rows to the dataset in AGO
                    #self.add_features(batch, i)
                    #self.logger.info(f'Example row: {batch[0]}')
                    t1 = Thread(target=self.add_features,
                                args=(list(split_batches[0]), i))
                    t2 = Thread(target=self.add_features,
                                args=(list(split_batches[1]), i))
                    t3 = Thread(target=self.add_features,
                                args=(list(split_batches[2]), i))
                    t4 = Thread(target=self.add_features,
                                args=(list(split_batches[3]), i))
                    t5 = Thread(target=self.add_features,
                                args=(list(split_batches[4]), i))
                    t6 = Thread(target=self.add_features,
                                args=(list(split_batches[5]), i))
                    t7 = Thread(target=self.add_features,
                                args=(list(split_batches[6]), i))
                    t8 = Thread(target=self.add_features,
                                args=(list(split_batches[7]), i))
                    t9 = Thread(target=self.add_features,
                                args=(list(split_batches[8]), i))
                    t10 = Thread(target=self.add_features,
                                args=(list(split_batches[9]), i))
                    t1.start()
                    t2.start()
                    t3.start()
                    t4.start()
                    t5.start()
                    t6.start()
                    t7.start()
                    t8.start()
                    t9.start()
                    t10.start()

                    t1.join()
                    t2.join()
                    t3.join()
                    t4.join()
                    t5.join()
                    t6.join()
                    t7.join()
                    t8.join()
                    t9.join()
                    t10.join()
                    self.logger.info('Batch added.')
                    adds = []
                    print(f'Duration: {time() - start}\n')
            # add leftover rows outside the loop if they don't add up to 4000
            if adds:
                start = time()
                self.logger.info(f'Adding last batch of {len(adds)}, at row #: {i+1}...')
                self.logger.info(f'Example row: {adds[0]}')
                self.add_features(adds, i)
                self.logger.info('Batch added.\n')
                print(f'Duration: {time() - start}')


        ago_count = self.layer_object.query(return_count_only=True)
        self.logger.info(f'count after batch adds: {str(ago_count)}')
        assert ago_count != 0


    def add_features(self, adds, on_row):
        '''
        Complicated function to wrap the edit_features arcgis function so we can handle AGO failing
        It will handle either:
        1. A reported rollback from AGO (1003) and try one more time,
        2. An AGO timeout, which can still be successful which we'll verify with a row count.
        '''
        def is_rolled_back(result):
            '''
            If we receieve a vague object back from AGO and it contains an error code of 1003
            docs:
            https://community.esri.com/t5/arcgis-api-for-python-questions/how-can-i-test-if-there-was-a-rollback/td-p/1057433
            '''
            if result["addResults"] is None:
                self.logger.info('Returned result not what we expected, assuming success.')
                self.logger.info(f'Returned object: {pprint(result)}')
                return True
            for element in result["addResults"]:
                if "error" in element and element["error"]["code"] == 1003:
                    return True
                elif "error" in element and element["error"]["code"] != 1003:
                    raise Exception(f'Got this error returned from AGO (unhandled error): {element["error"]}')
            return False

        def verify_count(on_row):
            '''If we receive a timeout, verify the count. It will sometimes have actually successfully added.'''
            count = self.layer_object.query(return_count_only=True)
            self.logger.info(f'Received a timeout, comparing row count {on_row+1} to {count} to see if we can continue')
            if int(on_row)+1 == int(count):
                return True
            else:
                self.logger.info('Counts dont match after AGO timeout...')
                return False

        success = False
        # save our result outside the while loop
        result = None
        while success is False:
            # Is it still rolled back after a retry?
            if result is not None:
                if is_rolled_back(result):
                    raise Exception("Retry on rollback didn't work.")

            # Add the batch
            try:
                result = self.layer_object.edit_features(adds=adds, rollback_on_failure=True)
            except Exception as e:
                if '504' in str(e):
                    # let's try ignoring timeouts for now, it seems the count catches up eventually
                    continue
                    sleep(5)
                    if verify_count(on_row):
                        success = True
                        continue
                    else:
                        raise e

            if is_rolled_back(result):
                self.logger.info("Results rolled back, retrying our batch adds....")
                try:
                    result = self.layer_object.edit_features(adds=adds, rollback_on_failure=True)
                except Exception as e:
                    if '504' in str(e):
                        # let's try ignoring timeouts for now, it seems the count catches up eventually
                        continue
                        if verify_count(on_row):
                            success = True
                            continue
                        else:
                            raise e
            # If we didn't get rolled back, batch of adds successfully added.
            else:
                success = True


    def verify_count(self):
        ago_count = self.layer_object.query(return_count_only=True)
        assert self._num_rows_in_upload_file == ago_count
        self.logger.info(f'CSV count matches AGO dataset, AGO: {str(ago_count)}, CSV: {str(self._num_rows_in_upload_file)}')


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
