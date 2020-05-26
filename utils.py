import boto3
import pymysql
import tarfile
import pandas as pd
import geopandas
import urllib.request
from shapely.geometry import Polygon
from sqlalchemy import create_engine

class TaxiDataUtils:
    def load_data(
        self, 
        filename, 
        tmpfolder, 
        downloadcsv=True, 
        csvcols=None,
        columnmaping=None,
        parse_dates=None,
        date_parser=None
    ):
        csvfile = '{}.csv'.format(filename)
        url = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_{}.csv'.format(filename)
        
        try:
            
            # загрузка файла
            if downloadcsv:
                print('{}: downloading...'.format(filename))
                urllib.request.urlretrieve(url, '{}{}'.format(tmpfolder, csvfile))
            
            # чтение файла
            print('{}: reading...'.format(filename))
            data = pd.read_csv(
                tmpfolder + csvfile,
                parse_dates=parse_dates,
                skipinitialspace=True,
                usecols=csvcols,
                date_parser=date_parser
            )
            
            # переименование полей
            if columnmaping is not None:
                data.rename(columns=columnmaping, inplace=True)
            
            # архивация файла
            if downloadcsv:
                print('{}: archiving...'.format(filename))
                with tarfile.open(
                    '{}{}.tar.gz'.format(tmpfolder, filename), 
                    'w:gz'
                ) as tar:
                    tar.add(tmpfolder + csvfile)
                
            # файл с координатами
            regions = pd.read_csv(tmpfolder + 'regions.csv', sep=';')
            
            # преобразуем координаты в полигоны
            geo_regions = geopandas.GeoDataFrame(
                geometry=[
                    Polygon([
                        (w, n), 
                        (e, n), 
                        (e, s), 
                        (w, s), 
                        (w, n)
                    ]) for w, e, s, n in zip(
                        regions.west, 
                        regions.east, 
                        regions.south, 
                        regions.north
                    )
                ]
            )
            
            # определим по координатам id региона
            print('{}: joining...'.format(filename))
            data['pickup_region_id'] = geopandas.tools.sjoin(
                geopandas.GeoDataFrame(
                    geometry=geopandas.points_from_xy(data.pickup_longitude, data.pickup_latitude)
                ), 
                geo_regions, 
                how="left",
                op='within'
            ).iloc[:, 1]
            
            # вычислим продолжительность поездки
            data['trip_duration'] = (
                (data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime'])
                .dt
                .total_seconds()
                .div(60)
                .astype(float)
            )
            
            # отбросим минуты и секунды во времени начала поездки
            data['tpep_pickup_datetime'] = data['tpep_pickup_datetime'].dt.floor('h')
            
            # фильтр по поездкам
            data.drop(
                data[
                    (data['passenger_count']==0)
                    | (data['trip_distance']==0) 
                    | (data['trip_duration']==0)
                    | (data['pickup_region_id'].isna())
                ].index,
                inplace=True
            )
            
            # агрегируем данные
            print('{}: aggregating...'.format(filename))
            aggregated_data = data.groupby(
                ['tpep_pickup_datetime', 'pickup_region_id'], as_index=False
            ).agg(
                {
                    'passenger_count': ['sum', 'mean', 'count'],
                    'trip_distance': ['mean'],
                    'total_amount': ['mean'],
                    'trip_duration': ['mean']
                }
            )

            aggregated_data.columns = [
                'pickup_datetime', 
                'pickup_region_id', 
                'passenger_count_sum', 
                'passenger_count_mean',
                'trip_count_sum',
                'trip_distance_mean',
                'total_amount_mean',
                'trip_duration_mean'
            ]
            
            # сохраним результат в файл
            print('{}: saving...'.format(filename))
            aggregated_data.to_csv(
                '{}{}_aggregated.csv'.format(tmpfolder, filename)
            )
            
        except Exception as e:
            print('Data load error: ', e)
            
        
    
    def create_aggregateddata_table(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute(
                'CREATE TABLE taxi_aggregateddata \
                ( \
                    pickup_datetime DATETIME, \
                    pickup_region_id INT, \
                    passenger_count_sum INT, \
                    passenger_count_mean FLOAT, \
                    trip_distance_mean FLOAT, \
                    total_amount_mean FLOAT, \
                    trip_duration_mean FLOAT, \
                    CONSTRAINT aggregateddata_pk PRIMARY KEY (pickup_datetime, pickup_region_id) \
                );'
            )
            conn.commit()
            print('Table taxi_aggregateddata created')
        finally:
            cursor.close()
    
    def create_regions_table(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute(
                'CREATE TABLE taxi_regions \
                ( \
                    region_id INT, \
                    west DOUBLE, \
                    east DOUBLE, \
                    south DOUBLE, \
                    north DOUBLE, \
                    region_poly POLYGON, \
                    CONSTRAINT regions_pk PRIMARY KEY (region_id) \
                );'
            )
            conn.commit()
            print('Table taxi_regions created')
        finally:
            cursor.close()
    
    def set_geo_polygons(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE \
                    taxi_regions \
                SET \
                    region_poly = ST_GEOMFROMTEXT( \
                        CONCAT( \
                            'POLYGON((', \
                            west, ' ', north, ', ', \
                            east, ' ', north, ', ', \
                            east, ' ', south, ', ', \
                            west, ' ', south, ', ', \
                            west, ' ', north, ' ', \
                            '))' \
                        ) \
                    );"
            )
            cursor.execute(
                'ALTER TABLE taxi_regions MODIFY region_poly POLYGON NOT NULL;'
            )
            cursor.execute(
                'CREATE SPATIAL INDEX sx_taxi_regions_geo ON taxi_regions(region_poly);'
            )
            conn.commit()
            print('Polygons created successfully')
        finally:
            cursor.close()

class AWSUtils:
    def __init__(self):
        self.s3_bucket_name = 'sagemaker-us-east-2-<hidden>'
        self.__db_host = 'mldb-cluster.cluster-<hidden>.us-east-2.rds.amazonaws.com'
        self.__db_port = 3306
        self.__db_name = 'mldb'
        self.__db_user = '<hidden>'
        self.__db_pass = '<hidden>'
    
    def connect_to_db(self):
        return pymysql.connect(
            host=self.__db_host, 
            port=self.__db_port, 
            user=self.__db_user, 
            passwd=self.__db_pass, 
            db=self.__db_name, 
            connect_timeout=30
        )
    
    def get_sqlalchemy_engine(self):
        return create_engine(
            'mysql+pymysql://{user}:{passwd}@{host}/{db}'
            .format(
                host=self.__db_host, 
                user=self.__db_user, 
                passwd=self.__db_pass, 
                db=self.__db_name,
            )
        )
    
    def get_s3_bucket(self):
        return boto3.resource('s3').Bucket(self.s3_bucket_name)
    
    def upload_to_s3(self, local_path, s3_path):
        self.get_s3_bucket().Object(s3_path).upload_file(local_path)
        print('{} uploaded successfully'.format(local_path))
        
    def download_from_s3(self, s3_path, local_path):
        self.get_s3_bucket().download_file(s3_path, local_path)
        print('{} downloaded successfully'.format(s3_path))
        
    def slq_execute(self, querystr):
        conn = self.connect_to_db()
        try: 
            with conn:
                cursor = conn.cursor()
                res = cursor.execute(querystr)
                conn.commit()
                return res
        finally:
            cursor.close()
            conn.close()
    
    def sql_function_execute(self, func):
        try:
            conn = self.connect_to_db()
            func(conn)
        finally:
            conn.close()
    
    def pd_read_sql(self, sql, **kwargs):
        try:
            conn = self.connect_to_db()
            return pd.read_sql(sql, conn, **kwargs)
        finally:
            conn.close()

if __name__ == '__main__':
    pass