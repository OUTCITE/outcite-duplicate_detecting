import sys
import sqlite3
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator

_inDB = sys.argv[1];

_scale = False;
# superset_size INT, patch_start INT, patch_end INT, MB_S INT, MB_all INT, t0 REAL, t1 REAL, t2 REAL, t3 REAL, t4 REAL, t5 REAL
_labels       = ['preparation','forking','processing','collecting results','getting struct'];
_labels_space = ['space struct','space all'];

con = sqlite3.connect(_inDB);
cur = con.cursor();

rows   = cur.execute("SELECT * FROM processing").fetchall();
sizes  = [row[0]                     for row in rows];
scales = [row[0]/rows[-1][0]         for row in rows];
points = [(row[1]+row[2])/2          for row in rows];
widths = [row[2]-row[1]              for row in rows];
spaces = [[-row[3]/1024,-(row[4]-row[3])/1024] for row in rows];
times  = [list(row[6:])              for row in rows];
sofart = [sum([time[-1] for time in times[:i]]) for i in range(len(times))];


def timestring(seconds):
    if seconds < 60:
        return str(int(seconds))+'s';
    elif seconds < 3600:
        mins = int(seconds/60);
        secs = int(seconds%60);
        return str(mins)+'m '+str(secs)+'s';
    else:
        hour = int(seconds/3600);
        mins = int((seconds%3600)/60);
        secs = int((seconds%3600)%60);
        return str(hour)+'h '+str(mins)+'m '+str(secs)+'s';

plt.figure(figsize=(50,2));

for i in range(len(times[0]))[::-1]:
    #plt.bar(points, [[1,scales[j]][_scale]*times[j][i] for j in range(len(times))], widths[i], label=_labels[i]);
    values = [[1,scales[j]][_scale]*times[j][i] for j in range(len(times))];
    #plt.plot(points, values, label=_labels[i]);
    plt.step(points, values, label=_labels[i], where='post', lw=0.001);#, color='black'
    if i > 0:
        prev_values = [[1,scales[j]][_scale]*times[j][i-1] for j in range(len(times))];
        plt.fill_between(points,prev_values,values,step='post');

for i in range(len(spaces[0]))[::-1]:
    #plt.step(points, [[1,scales[j]][_scale]*spaces[j][i] for j in range(len(spaces))], widths[i], label=_labels_space[i]);
    values = [[1,scales[j]][_scale]*spaces[j][i] for j in range(len(spaces))];
    plt.step(points, values, label=_labels_space[i], where='post');
    plt.fill_between(points,0,values,step='post');
    #plt.plot(points, [[1,scales[j]][_scale]*spaces[j][i] for j in range(len(spaces))], label=_labels_space[i]);

oom = sum([time[-1] for time in times]) / len(times);
top = ([1,scales[-1]][_scale]*times[0][-1]) + 0.075*oom;
plt.vlines([points[0]],0,top,color='black');
plt.annotate('['+str(sizes[0])+']  '+timestring(sofart[0]),(points[0],top),xytext=(2,-5),textcoords='offset points',fontsize='x-small',rotation='vertical'); #arrowprops={'arrowstyle':'->'}
for i in range(1,len(sizes)):
    if sizes[i] > sizes[i-1]:
        top = ([1,scales[-1]][_scale]*times[i][-1]) + 0.075*oom;
        plt.vlines([points[i]],0,top,color='black');
        plt.annotate('['+str(sizes[i])+']  '+timestring(sofart[i]),(points[i],top),xytext=(2,-5),textcoords='offset points',fontsize='x-small',rotation='vertical'); #arrowprops={'arrowstyle':'->'}

plt.xlabel('potential supersets');
plt.ylabel('runtime in seconds');
leg = plt.legend(fontsize='x-small',ncol=2);
for legobj in leg.legendHandles:
    legobj.set_linewidth(4.0);
plt.savefig(_inDB+'.report.png',dpi=600);
