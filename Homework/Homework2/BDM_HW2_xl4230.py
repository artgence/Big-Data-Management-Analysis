import csv
import re
import sys
import mapreduce as mr
from datetime import date
from mrjob.job import MRJob
from mrjob.step import MRStep

################################
### YOUR WORK SHOULD BE HERE ###
################################
class MRHW2(MRJob):
    '''
    PLEASE COMPLETE THIS CLASS. THIS SHOULD BE THE ONLY PLACE THAT YOU CAN EDIT.
    THE INPUT OF YOUR MAPREDUCE JOB WOULD BE LINE OF TEXT WITHOUT '\n'.
    '''
    def mapper(self, _, line):
      info = line.split('[')
      placekey = info[0].split(',')[0]
      NYC = ['New York', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
      if any(city in info[0] for city in NYC):
        date_range_start = re.search(r'\d{4}-\d{2}-\d{2}', info[0]).group()
        visit_by_day = info[1].split(']')[0].split(',')
        yield (placekey, (date_range_start, visit_by_day))

    def combiner(self, _, start_time_and_visit_by_day):
      visit_flag = [False, False, False, False, False, False]

      for date_range_start, visit_by_day in start_time_and_visit_by_day:
        visit_by_day = list(map(int, visit_by_day))
        day = date_range_start.split('-')
        diff = date(int(day[0]), int(day[1]), int(day[2])) - date(2020,3,2)

        if diff.days < 14 or (diff.days == 14 and visit_by_day[0] > 0): visit_flag[0] = True
        if diff.days < 28 or (diff.days == 28 and sum(visit_by_day[:2]) > 0): visit_flag[2] = True
        if diff.days > 14 or (diff.days == 14 and sum(visit_by_day[1:]) > 0): visit_flag[1] = True
        if diff.days == 28 and sum(visit_by_day[2:]) > 0: visit_flag[3] = True
        if diff.days == 14: visit_flag[4] = True
        if diff.days == 28: visit_flag[5] = True

      by17, by1 = 0, 0
      if (visit_flag[0] is True and visit_flag[1] is False and visit_flag[4] is True): by17 = int(1)
      if (visit_flag[2] is True and visit_flag[3] is False and visit_flag[5] is True): by1 = int(1)
      
      if by17 == 1:  yield 'March 17, 2020', by17
      if by1 == 1:  yield 'April 01, 2020', by1
      

    def reducer(self, times, types):
      yield "The number of restaurants in NYC closed by " + times, sum(types)

if __name__ == '__main__':
    job = MRHW2(args=[])
    with open(sys.argv[1], 'r') as fi:
      next(fi)
      output = list(mr.runJob(enumerate(map(lambda x: x.strip(), fi)), job))
    output.reverse()
    for i in output:
      print(i, sep='\n')