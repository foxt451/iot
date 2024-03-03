from csv import reader
from datetime import datetime
from typing import Dict, Tuple
from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.gps import Gps
from domain.parking import Parking


class FileDatasource:
    def __init__(self, filenames: Dict[str, str]) -> None:
        self.filenames = filenames
        self.row_counters = {datatype: 0 for datatype in filenames}
        self.file_contents = {datatype: [] for datatype in filenames}

    def read(self):
        data = {}
        for datatype in self.filenames:
            self.row_counters[datatype] %= len(self.file_contents[datatype])
            data[datatype] = self.file_contents[datatype][self.row_counters[datatype]]
            self.row_counters[datatype] += 1
        return data

    def startReading(self, *args, **kwargs):
        for datatype in self.filenames:
            with open(self.filenames[datatype], "r") as file:
                self.file_contents[datatype] = list(reader(file))[1:]

    def stopReading(self, *args, **kwargs):
        self.row_counters = {datatype: 0 for datatype in self.filenames}


class AggregatedFileDatasource(FileDatasource):
    def __init__(
        self, accelerometer_filename: str, gps_filename: str, parking_filename: str
    ) -> None:
        super().__init__(
            {
                "accelerometer": accelerometer_filename,
                "gps": gps_filename,
                "parking": parking_filename,
            }
        )

    def read(self) -> AggregatedData:
        """Метод повертає дані отримані з датчиків"""
        data = super().read()
        time = datetime.now()
        accelerometer = Accelerometer(
            x=int(data["accelerometer"][0]),
            z=int(data["accelerometer"][2]),
            y=int(data["accelerometer"][1]),
        )
        gps = Gps(
            latitude=float(data["gps"][0]),
            longitude=float(data["gps"][1]),
        )
        return AggregatedData(accelerometer, gps, time) 


class ParkingFileDatasource(FileDatasource):
    def __init__(self, parking_filename: str) -> None:
        super().__init__({"parking": parking_filename})

    def read(self) -> Parking:
        """Метод повертає дані про парковку"""
        data = super().read()
        return Parking(gps=Gps(latitude=float(data["parking"][0]), longitude=float(data["parking"][1])), empty_count=int(data["parking"][2]))
