import MySQLdb as mdb
import sys
from collections import defaultdict,OrderedDict
import json

class Analyzeyelpdata:
   def __init__(self):
      self.week_day={0:'Monday',1:'Tuesday',2:'Wednesday',3:'Thursday',4:'Friday',5:'Saturday',6:'Sunday'}
      self.business_datasize=11537
      self.datasize={'checkin': 8282, 'user': 43873, 'review': 229907, 'business': 11537}
      self.db='yelpdatabase'

   def review_count(self, table='review', Idmin=1, Idmax=1):
       try:
          con = mdb.connect('localhost', 'mysqlu','')
          cur = con.cursor()
          cur.execute("use %s"%(self.db))     
          num_reviews=defaultdict(float)
          num_reviews_categories={}

          for x in self.week_day.keys():
              cur.execute('select count(*) from %s where weekday="%s" and (Id<%d or Id>=%d)'%(table, self.week_day[x],Idmin,Idmax))
              num_reviews[self.week_day[x]],= cur.fetchone()
          total=sum(num_reviews.values())
          for x in self.week_day.values():
             num_reviews[x]=round(num_reviews[x]/float(total),5)*100.0
          #print num_reviews,1/7.0
          num_reviews_categories['total']= num_reviews

          cur.execute('select distinct categories from %s'%(table))
          categories=cur.fetchall()
          for (category,) in categories:
                tmp=defaultdict(float)
                cur.execute("select count(*) from %s where categories='%s' and (Id<%s or Id>=%s)"%(table, category, Idmin, Idmax))
                total,=cur.fetchone()
                cur.execute("select weekday,count(*) from %s where categories='%s' and (Id<%s or Id>=%s) group by weekday"%(table,category, Idmin, Idmax))
                for key,value in cur.fetchall():
                     tmp[key]=round(value/float(total),5)*100.0
                num_reviews_categories[category]=tmp

       except mdb.Error, e:
          print "Error %d: %s" % (e.args[0],e.args[1])
          sys.exit(1)          
       finally:
          if cur: cur.close()
          if con: con.close()
       return num_reviews_categories

   def checkin_count(self, table='checkin', Idmin=1, Idmax=1):
       try:
          con = mdb.connect('localhost', 'mysqlu','')
          cur = con.cursor()
          cur.execute("use %s"%(self.db))     
          num_checkin=defaultdict(float)
          num_checkin_categories={}

          for x in self.week_day.keys():
              cur.execute('select cast(sum(checkin_no) as unsigned) from %s where weekday="%s" and (Id<%d or Id>=%d)'%(table, self.week_day[x], Idmin, Idmax))
              num_checkin[self.week_day[x]],= cur.fetchone()
          total=sum(num_checkin.values())
          for x in self.week_day.values():
             num_checkin[x]=round(num_checkin[x]/float(total),5)*100.0
          #print num_reviews,1/7.0
          num_checkin_categories['total']= num_checkin

          cur.execute('select distinct categories from %s'%(table))
          categories=cur.fetchall()
          for (category,) in categories:
                tmp=defaultdict(float)
                cur.execute("select cast(sum(checkin_no) as unsigned) from %s where categories='%s' and (Id<%s or Id>=%s)"%(table, category, Idmin, Idmax))
                total,=cur.fetchone()
                cur.execute("select weekday,cast(sum(checkin_no) as unsigned) from %s where categories='%s' and (Id<%s or Id>=%s) group by weekday"%(table,category, Idmin, Idmax))
                for key,value in cur.fetchall():
                     tmp[key]=round(value/float(total),5)*100.0
                num_checkin_categories[category]=tmp

       except mdb.Error, e:
          print "Error %d: %s" % (e.args[0],e.args[1])
          sys.exit(1)          
       finally:
          if cur: cur.close()
          if con: con.close()
       return num_checkin_categories

   # doing jacknife to esimate the error in mean
   def mean_and_error(self, analyzedb, mean, case):
       max=8 # divide the data into this many chunks to do jacknife
       # initialize dict to store mean and jacknife error
       result = defaultdict(lambda: defaultdict(float))
       #analyzedb=Analyzeyelpdata()
       size= int(self.datasize[case]/max)
       for i in xrange(1,max+1):
           # for each chunk, remove that from the full dataset and compute the mean 
           var=analyzedb.review_count(Idmin=(i-1)*size,Idmax=i*size)
           for category,item in var.items():
              for weekday,val in item.items():  
                  result[category][weekday]+=(val-mean[category][weekday])**2
       for category,item in result.items():
          for weekday,val in item.items():
              result[category][weekday]=(mean[category][weekday], ((max-1.0)/max*result[category][weekday])**0.5)              
              #print category,weekday,result[category][weekday][0],result[category][weekday][1]
       return result
   
   # this method generate plot showing the histogram and the error for review and checkin
   def plot_barchart(self, result,start = 0,end = 7,case='review'):
      import numpy as np
      import matplotlib.pyplot as plt
      colors=['#CCFF66','#99CCFF','#9999CC','#FFCCCC','#FFCC66','#CC9900','#FF6666']
      weekdays=self.week_day.values()
      print weekdays
      num_weekdays=7
      categories=['total','Active Life', 'Arts & Entertainment', 'Automotive', 'Beauty & Spas', 'Event Planning & Services', 'Food', 'Health & Medical', 'Home Services',  'Hotels & Travel', 'Local Services',  'Nightlife', 'Pets', 'Restaurants', 'Shopping']
      category_labels=['total','Active\n Life', 'Arts &\n Entertainment', 'Automotive', 'Beauty &\n Spas', 'Event Planning\n & Services', 'Food', 'Health &\n Medical', 'Home \n Services',  'Hotels &\n Travel', 'Local\n Services',  'Nightlife', 'Pets', 'Restaurants', 'Shopping']
      num_categories=len(categories)
      if end=='end': end = num_categories
      how_many=end-start     
      ind=np.arange(how_many)
      print ind, categories, how_many
      margin=0.1
      width=(1.-2.*margin)/how_many

      #mean,std=[],[]
      #for weekday in weekdays:
      #   mean.append(result['total'][weekday][0])
      #   std.append(result['total'][weekday][1])      
      mean,std= np.zeros(num_weekdays*how_many).reshape(num_weekdays,how_many), np.zeros(num_weekdays*how_many).reshape(num_weekdays,how_many) # create 2D array to store mean and std to plot,

      # fill up mean and variance matrix below 
      column=0
      for i in xrange(start,end):
          row=0
          for weekday in weekdays:
               mean[row][column]=result[categories[i]][weekday][0]
               std[row][column]=result[categories[i]][weekday][1]
               row+=1
          column+=1
      mean=mean.tolist()
      std=std.tolist()

      plt.figure(figsize=(16,6),dpi=300)     
      s = plt.subplot(1,1,1)
      legend_data=[]
      for num, (mean_vals, std_vals) in enumerate(zip(mean, std)):
             xdata=ind+margin+num*width
             rect=plt.bar(xdata, mean_vals, width, edgecolor="none", color=colors[num])
             plt.errorbar(xdata+width/2, mean_vals, yerr=std_vals, fmt='ko')
             legend_data.append(rect[0])
      xdata=np.arange(end-start+1)       
      linedata=plt.plot(xdata,100.0/7.0*np.ones(len(xdata)),'--k',linewidth=2)
      legend_data.append(linedata[0])
      legend_data=tuple(legend_data)

      s.set_xticks(ind+0.5)
      s.set_xticklabels(category_labels[start:end])
      if case=='review': s.set_ylim([0,23])
      if case=='checkin': s.set_ylim([0,33])
      plt.legend( legend_data, tuple(weekdays)+('equally likely',),ncol=4 )
      if case=='checkin':
            plt.ylabel(r'% checkins')
            plt.savefig('checkin.svg',transparent=True, bbox_inches='tight', pad_inches=0.1)
      if case=='review':
            plt.ylabel(r'% reviews written')
            plt.savefig('review1.svg',transparent=True, bbox_inches='tight', pad_inches=0.1)


      #plt.show()      
 

if __name__ == "__main__":
   analyzedb=Analyzeyelpdata()
   
   mean=analyzedb.review_count()
   result=analyzedb.mean_and_error(analyzedb, mean,'review')
   #print json.dumps(result,indent=4,sort_keys=True)
   # use end='end' if want to include till the last feature
   analyzedb.plot_barchart(result,start=7,end='end',case='review')
   

   """
   mean=analyzedb.checkin_count()   
   result=analyzedb.mean_and_error(analyzedb, mean,'checkin')
   #print json.dumps(result,indent=4,sort_keys=True)
   # use end='end' if want to include till the last feature
   analyzedb.plot_barchart(result,start=0,end=7,case='checkin')
   """
