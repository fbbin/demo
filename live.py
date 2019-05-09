#!/usr/bin/env python
#coding:utf-8
# Author:
# Purpose: 视频推流和第三流转推
# Created: 2014/10/07

import copy
import tornado.ioloop
import tornado.web
import logging
import threading
import subprocess
import json
import os
import logging.config
import time
import platform
import base64
import urllib2
import datetime

from daemonize import daemonize

FFMPEG  = "/root/bin/ffmpeg-pic"
RTMPDUMP_PUSH = "/root/xuxiandi/rtmpdump-push/rtmpdump"

FLVPATH = "/udisk/www/videonew/newrecord"

if not platform.system() == "Windows":
    UPLOAD_FILE_PATH = "/udisk/www/video/pushvod/movieupload/zhanqi"
else:
    UPLOAD_FILE_PATH = "d:\\riyo-live"

STATIC_PATH = "/root/xuxiandi/rebroadcastall/static"

mylock1 = threading.RLock()             # type1线程锁
mylock2 = threading.RLock()             # type2线程锁

runstatus = {}
livelist  = {}            # 全局转推队列格式id:[srcaddr,desaddr,type]
flvlist   = {}            # 全局视频转推队列格式id:[(flvs),desaddr]
timecloselist = {}        # 重播定时关闭列表格式id:[stoptime]

#网宿视频下载通知回调接口数据
test_callback_data = """
{
"items": [
        {
            "persistentId": "ezCFDDEFFAZ12445",
            "streamname": "161009_0wysW",
            "ops": "create",
            "bucket": "zhanqi",
            "code": "200",
            "desc": "ok",
            "error": "success",
            "keys": [ "zqlive-161009_0wysW--20160805192959.flv",
                      "zqlive-161009_0wysW--20160805193707.flv"
            ],
            "urls": [ "http://wswcsvod.zhanqi.8686c.com/zqlive-161009_0wysW--20160805192959.flv",
                      "http://wswcsvod.zhanqi.8686c.com/zqlive-161009_0wysW--20160805193707.flv"
            ]
        }
    ]
}

"""

#http://127.0.0.1:7381/wsvideonotify?message_type=ws_record_finish   ewoiaXRlbXMiOiBbCiAgICAgICAgewogICAgICAgICAgICAicGVyc2lzdGVudElkIjogImV6Q0ZEREVGRkFaMTI0NDUiLAogICAgICAgICAgICAic3RyZWFtbmFtZSI6ICIxNjEwMDlfMHd5c1ciLAogICAgICAgICAgICAib3BzIjogImNyZWF0ZSIsCiAgICAgICAgICAgICJidWNrZXQiOiAiemhhbnFpIiwKICAgICAgICAgICAgImNvZGUiOiAiMjAwIiwKICAgICAgICAgICAgImRlc2MiOiAib2siLAogICAgICAgICAgICAiZXJyb3IiOiAic3VjY2VzcyIsCiAgICAgICAgICAgICJrZXlzIjogWyAienFsaXZlLTE2MTAwOV8wd3lzVy0tMjAxNjA4MDUxOTI5NTkuZmx2IiwKICAgICAgICAgICAgICAgICAgICAgICJ6cWxpdmUtMTYxMDA5XzB3eXNXLS0yMDE2MDgwNTE5MzcwNy5mbHYiCiAgICAgICAgICAgIF0sCiAgICAgICAgICAgICJ1cmxzIjogWyAiaHR0cDovL3dzd2Nzdm9kLnpoYW5xaS44Njg2Yy5jb20venFsaXZlLTE2MTAwOV8wd3lzVy0tMjAxNjA4MDUxOTI5NTkuZmx2IiwKICAgICAgICAgICAgICAgICAgICAgICJodHRwOi8vd3N3Y3N2b2QuemhhbnFpLjg2ODZjLmNvbS96cWxpdmUtMTYxMDA5XzB3eXNXLS0yMDE2MDgwNTE5MzcwNy5mbHYiCiAgICAgICAgICAgIF0KICAgICAgICB9CiAgICBdCn0=

wsRecordDownloadListLock = threading.RLock() #网宿云视频下载列表
wsRecordDownloadList = {}  #网宿视频下载列表

uploadFileList = []       # 服务器上传视频列表[("dir1/upload1.mp4", 13443), ("dir2/upload2.mp4", 14992)]

uploadFileListJsonLock = threading.RLock()  # 文件列表刷新锁
uploadFileListJson = "" # 服务器上传视频列表,已转换成json格式


def recurse_list(parentDir):
    global uploadFileList

    listdir = os.listdir(parentDir)
    for each in listdir:
        fullPath = os.path.join(parentDir, each)
        if os.path.isfile(fullPath):
            if each.endswith(".mp4") or each.endswith(".flv"):
                file_tm = int(os.path.getmtime(fullPath))
                uploadFileList.append((fullPath[len(parentDir):], file_tm))

        elif os.path.isdir(fullPath):
            recurse_list(fullPath)

def update():
    global uploadFileList
    global uploadFileListJson

    dic_dirs = dict()
    dic_dirs["dirs"] = []

    listdir = os.listdir(UPLOAD_FILE_PATH)
    for each in listdir:
        fullPath = os.path.join(UPLOAD_FILE_PATH, each)
        if os.path.isdir(fullPath):
            dic_dir = dict()
            dic_dir["name"] = each
            dic_dir["files"] = []

            uploadFileList = []
            recurse_list(fullPath)

            for each_file in uploadFileList:
                file_dic = dict()
                file_dic["name"] = each_file[0]
                file_dic["tm"] = each_file[1]
                dic_dir["files"].append(file_dic)

            dic_dirs["dirs"].append(dic_dir)

    with uploadFileListJsonLock:
        uploadFileListJson = json.dumps(dic_dirs)

class TimeCloseVideoThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger("rebroadcastall")

    def close_room(self, rid):
        try:
            p = self.runlist[rid]
            p.terminate()
            time.sleep(0.5)
            # return
        except Exception, e:
            self.loger.error("%s结束失败%s" % (rid, e))


    def run(self):
        global timecloselist
        try:
            if len(timecloselist) > 0:
                close_ids = []
                now_time = time.time()
                for room_id, time_close in timecloselist.items():
                    if time_close > now_time:
                        close_ids.append(room_id)
                        self.close_room(room_id)

                if len(close_ids) > 0:
                    for id in close_ids:
                        del timecloselist[id]

            time.sleep(5)
        except Exception as e:
            self.logger.warning("定时关闭房间重播线程异常. exception:{}".format(e))


class UploadFileDirMonitor(threading.Thread):
    def __init__(self, path):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger("rebroadcastall")
        self.monitorPath = path

    def run(self):
        self.FsMonitor(self.monitorPath)

    def FsMonitor(self, path='.'):
        self.logger.info("now starting monitor %s." % path)

        while True:
            try:
                update();
                time.sleep(3)
            except Exception as e:
                self.logger.error("FsMonitor exception. e:{0}".format(e))

        self.logger.info("stop monitor %s." % path)

class ListUploadFilesHandler(tornado.web.RequestHandler):
    '''
       获取可以拿来直播的文件列表，指定目录，由运营上传视频文件(flv, mp4)
       天枢后台可操作进行播放
    '''

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def get(self):
        global uploadFileListJson
        global uploadFileListJsonLock

        with uploadFileListJsonLock:
            self.write(uploadFileListJson)

####http接口处理###
class StartHandler(tornado.web.RequestHandler):
    '''处理start请求，请求格式
    type:1 录制视频推直播,loopplay是否支持循环
    type:2 第三方流转推, 使用ffmpeg
    type:3 第三方流转推，使用rtmpump-push
    {"id":1,"type":1,"srcaddr":['1.flv','2.flv'],"desaddr":"rtmp://ip/app/111","loopplay":1}
    {"id":1,"type":2,"srcaddr":"rtmp://ip/app/22","desaddr":"rtmp://ip/app/111"}
    {"id":1,"type":3,"srcaddr":"rtmp://ip/app/22","desaddr":"rtmp://ip/app/111"}
    '''
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def isEmpty(self, json, key):
        return not json.get(key, 0)

    def post(self):
        global livelist #转推任务列表
        global flvlist  #视频任务列表
        self.loger=logging.getLogger("rebroadcastall")
        raw_data = self.request.body
        self.loger.info(u'start接口请求参数为%s'%raw_data)

        try:
            raw_data_json=json.loads(raw_data)
        except:
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return
        if not type(raw_data_json)==type({}):
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return

        try:
            posttype = raw_data_json.get('type','9')
        except:
            self.loger.error(u"提交数据缺少参数%s"%raw_data)
            self.write({"code":2,"msg":"提交数据缺少参数"})
            return

        if int(posttype) == 1:
            #######
            if  self.isEmpty(raw_data_json,'id') or self.isEmpty(raw_data_json, 'srcaddr') or self.isEmpty(raw_data_json,'desaddr'):
                self.loger.error(u"提交数据缺少参数%s"%raw_data)
                self.write({"code":2,"msg":"提交数据缺少参数"})
                return

            id = str(raw_data_json.get('id'))
            try:
                srcaddr=raw_data_json.get('srcaddr',0)
            except:
                self.loger.error(u"提交数据srcaddr参数不合法%s"%raw_data)
                self.write({"code":3,"msg":"提交数据srcaddr参数不合法"})

            if not (type(srcaddr) == type([])):
                self.loger.error(u"提交数据srcaddr参数不合法%s"%raw_data)
                self.write({"code":3,"msg":"提交数据srcaddr参数不合法"})
                return

            desaddr= raw_data_json.get('desaddr')
            if not (desaddr.startswith('rtmp://')):
                self.loger.error(u"提交数据desaddr参数不合法%s" %raw_data)
                self.write({"code":3,"msg":"提交数据desaddr参数不合法"})
                return

            loopplay = int(raw_data_json.get('loopplay',0))
            if loopplay == 0:
                self.loger.info(u'%s-%s需要循环播放' %(id,srcaddr))
                for i in range(6):
                    srcaddr.extend(srcaddr)

            if loopplay != 0:
                self.loger.info(u'%s-%s需要循环播放%d次' %(id,srcaddr,loopplay))
                srcaddr1 = srcaddr
                for i in range(loopplay-1):
                    srcaddr.extend(srcaddr1)

            if not (id in flvlist):
                #flvlist[id]=(srcaddr,desaddr)

                self.write({"code":0,"msg":"succeed"})
                ####增加检测逻辑，如果desaddr存在相同的就关掉原来的#####
                flvlisttmp={}
                flvlisttmp = copy.deepcopy(flvlist)
                for k,v in flvlisttmp.items():
                    if desaddr == v[1]:
                        try:
                            mylock1.acquire()
                            del flvlist[k]
                            mylock1.release()
                            self.loger.info("%s已经存在" %desaddr)
                        except:
                            pass
                        time.sleep(5)
                mylock1.acquire()
                flvlist[id] = (srcaddr, desaddr)
                mylock1.release()
                self.loger.info(u'增加视频任务队列%s，%s' %(id,str(flvlist[id])))
                return
            else:
                self.write({"code":4,"msg":"重复任务"})
                return

        elif int(posttype) == 2 or int(posttype) == 3:
            if self.isEmpty(raw_data_json, 'id') or self.isEmpty(raw_data_json, 'srcaddr') or self.isEmpty(raw_data_json, 'desaddr'):
                self.loger.error(u"提交数据缺少参数%s"%raw_data)
                self.write({"code":2,"msg":"提交数据缺少参数"})
                return

            id = str(raw_data_json.get('id'))
            srcaddr = raw_data_json.get('srcaddr')
            if not (srcaddr.startswith('rtmp://') or srcaddr.startswith('http://')):
                self.loger.error(u"提交数据srcaddr参数不合法%s"%raw_data)
                self.write({"code":3,"msg":"提交数据srcaddr参数不合法"})
                return

            desaddr= raw_data_json.get('desaddr')
            if not (desaddr.startswith('rtmp://')):
                self.loger.error(u"提交数据desaddr参数不合法%s" %raw_data)
                self.write({"code":3,"msg":"提交数据desaddr参数不合法"})
                return

            for k,v in livelist.items():
                if desaddr== v[1]:
                    self.loger.error(u"提交数据推流地址已经存在%s" %raw_data)
                    self.write({"code":3,"msg":"推流地址已存在"})
                    return

            if not (id in livelist):
                mylock2.acquire()
                livelist[id] =[srcaddr, desaddr, int(posttype)]
                mylock2.release()
                self.loger.info(u'增加任务队列%s，%s' %(id,str(livelist[id])))
                self.write({"code":0,"msg":"succeed"})
                return
            else:
                self.write({"code":4,"msg":"重复任务"})
                return

        self.write({"code":5,"msg":"无效type"})
        return

class StopHandler(tornado.web.RequestHandler):
    '''处理stop请求，请求格式
        type:1 录制视频推直播
        type:2 第三方流转推
        {"id":1,"type":1}
        {"id":1,"type":2}
    '''

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def post(self):
        global livelist #转推任务列表
        global flvlist  #视频任务列表
        global runstatus

        self.loger=logging.getLogger("rebroadcastall")
        raw_data = self.request.body
        self.loger.info(u'stop接口请求参数为%s'%raw_data)
        try:
            raw_data_json=json.loads(raw_data)
        except:
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return
        if not type(raw_data_json)==type({}):
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return
        try:
            posttype = raw_data_json.get('type', '9')
        except:
            self.loger.error(u"提交数据缺少参数%s"%raw_data)
            self.write({"code": 2, "msg": "提交数据缺少参数"})
            return
        
        if int(posttype) == 1:
            if not (raw_data_json.get('id', 0)):
                self.loger.error(u"提交数据缺少参数%s"%raw_data)
                self.write({"code":2,"msg":"提交数据缺少参数"})
                return
            id = str(raw_data_json.get('id'))
            
            if id in flvlist:
                self.loger.info(u'清除任务队列%s,%s' %(id,str(flvlist[id])))
                mylock1.acquire()
                del flvlist[id]

                try:
                    if id in runstatus:
                        del runstatus[id]
                except Exception as e :
                        self.loger.error(u"%s不存在%s, Exception:%s" % (runstatus, id, e))

                mylock1.release()
                self.write({"code": 0, "msg": "succeed"})
                return
            else:
                self.loger.error(u'任务队列%s不存在' %id)
                self.write({"code":6,"msg":"%s不存在" %id})
                return
        elif int(posttype) == 2 or int(posttype) == 3:
            id = str(raw_data_json.get('id'))
            if id in livelist:
                self.loger.info(u'清除任务队列%s,%s' %(id,str(livelist[id])))
                mylock2.acquire()
                del livelist[id]
                mylock2.release()
                try:
                    del runstatus[id]
                except:
                    self.loger.error(u"%s不存在%s"%(runstatus, id))
                time.sleep(2)
                self.write({"code":0,"msg":"succeed"})
                return
            else:
                self.loger.error(u'任务队列%s不存在' %id)
                self.write({"code":6,"msg":"任务队列%s不存在" %id} )
                return
        self.write({"code":5,"msg":"无效type"})
        time.sleep(2)
        return


class TimeStopHandler(tornado.web.RequestHandler):
    """
       定时关闭重播接口
       {"id":133322, "closetime":"2017-01-19 18:02:03"}
    """

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def post(self):
        global timecloselist

        self.logger = logging.getLogger("rebroadcastall")

        raw_data = self.request.body
        self.logger.debug(u'定时关播请求接口%s'%raw_data)

        try:
            raw_data_json=json.loads(raw_data)
        except Exception as e:
            self.logger.error(u"提交数据不是josn格式%s exception:%s" % (raw_data, e))
            self.write({"code":1,"msg":"请提交josn格式"})
            return

        if not type(raw_data_json)==type({}):
            self.logger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return

        try:
            room_id = raw_data_json.get('id', '0')
        except:
            self.logger.error(u"提交数据缺少参数%s" % raw_data)
            self.write({"code": 2, "msg": "提交数据缺少参数"})
            return

        try:
            close_time_str = raw_data_json.get('closetime', '')
            if len(close_time_str) > 0:
                close_time = time.mktime(time.strptime(close_time_str, '%Y-%m-%d %H:%M:%S'))
            else:
                raise Exception("time format error")
        except:
            self.logger.error(u"提交数据缺少参数%s" % raw_data)
            self.write({"code": 2, "msg": "提交数据缺少参数"})
            return

        timecloselist[room_id] = close_time
        self.logger.info("添加定时关闭成功. id:{} time:{}".format(room_id, close_time_str))

        self.set_status(200)
        self.write("OK")


class StatusHandler(tornado.web.RequestHandler):
    """查看状态接口"""
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    #----------------------------------------------------------------------
    def post(self):
        """Constructor"""
        global livelist #转推任务列表
        global flvlist  #视频任务列表
        global runstatus
        self.loger=logging.getLogger("rebroadcastall")
        raw_data = self.request.body
        self.loger.debug(u'status接口请求参数为%s'%raw_data)
        try:
            raw_data_json=json.loads(raw_data)
        except:
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return
        if not type(raw_data_json)==type({}):
            self.loger.error(u"提交数据不是josn格式%s"%raw_data)
            self.write({"code":1,"msg":"请提交josn格式"})
            return
        try:
            posttype = raw_data_json.get('type','9')
        except:
            self.loger.error(u"提交数据缺少参数%s"%raw_data)
            self.write({"code":2,"msg":"提交数据缺少参数"})
            return

        if int(posttype) == 1:
            self.write(runstatus)
            return
        if int(posttype) == 2 or int(posttype) == 3:
            self.write(livelist)
            return

        self.write({"code":5,"msg":"无效type"})
        #time.sleep(2)
        return

    #----------------------------------------------------------------------
    def get(self):
        """"""
        global livelist
        global flvlist
        global runstatus
        self.write('live:%s\n' %(livelist))
        self.write('flv:%s\n' %(flvlist))

###########逻辑处理####
class RebroadcastThread(threading.Thread):
    '''第三方流转推线程'''
    def __init__(self):
        threading.Thread.__init__(self)
        self.loger=logging.getLogger("rebroadcastall")
        global FFMPEG
        self.ffmpeg = FFMPEG

        global livelist   #全局任务列表
        self.livelist = livelist

        global RTMPDUMP_PUSH
        self.rtmpdump_push = RTMPDUMP_PUSH

        self.runlist = {} #运行数据列表
        self.exit   = False

    #----------------------------------------------------------------------
    def StartCastLive(self, pid, type, srcaddr, desaddr):
        """处理开启流推流"""
        if pid in self.runlist:
            #self.loger.info(u'%s已经在运行队列中'%pid)
            return

        if type == 2:
            command = [self.ffmpeg, "-re", "-i", srcaddr, "-acodec", "copy", "-vcodec", "copy", "-f", "flv", desaddr]
        else:
            command = [self.rtmpdump_push, "-v", "-r", srcaddr, "-P", desaddr]

        p = subprocess.Popen(command)
        self.loger.info(u'start接口开启线程%s,命令%s' %(pid,command))
        time.sleep(2)
        self.runlist[pid] = p
        if not self.runlist[pid]:
            self.loger.error(u"%s进程开启失败" % pid)

    def StopCastLive(self,rid):
        """处理开启流推流"""
        try:
            p=self.runlist[rid]
            p.terminate()
            time.sleep(0.5)
            #return
        except Exception,e:
            self.loger.error("%s结束失败%s" %(rid,e))
            #return


    def run(self):
        """执行函数"""
        while True:
            rl=[] #初始化变量
            livelisttmp={} #self.livelist临时变量
            mylock2.acquire()
            livelisttmp =copy.deepcopy(self.livelist)
            mylock2.release()

            if livelisttmp:
                for n in livelisttmp:
                    pid = n
                    srcaddr = livelisttmp[n][0]
                    desaddr = livelisttmp[n][1]
                    type    = int(livelisttmp[n][2])
                    
                    self.StartCastLive(pid, type, '"%s"' % srcaddr, desaddr)

            if self.runlist:
                mylock2.acquire()
                for k, v in self.runlist.items():
                    rl.append(k)
                mylock2.release()

                for rid in rl:
                    if not (rid in self.livelist):
                        self.loger.info(u'%s已经在任务队列删除' %rid)
                        self.StopCastLive(rid)
                    if not (self.runlist[rid].poll() is None):
                        self.loger.error(u"%s进程已经挂掉" %rid)
                        try:
                            del self.runlist[rid]
                        except  Exception as e:
                            self.loger.error(u"%s进程挂掉断允许列表删除失败%s" %(rid,e))
                print self.runlist
            time.sleep(1)

class WSVideoRecordDownloadThread(threading.Thread):
    def __init__(self, item):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger("rebroadcastall")
        self.item = item
        self.persistentId = self.item["persistentId"]
        self.keys = self.item["keys"]
        self.urls = self.item["urls"]

    def run(self):
        global wsRecordDownloadListLock
        global wsRecordDownloadList
        try:
            for index, item in enumerate(self.keys):
                self.download(self.keys[index], self.urls[index])

        except Exception as e:
            self.logger.error("WSVideoRecord download thread error. e:{0}".format(e))

        with wsRecordDownloadListLock:
            if self.persistentId in wsRecordDownloadList:
                del wsRecordDownloadList[self.persistentId]
            self.logger.info("Remove download thread. data:{0}".format(self.item))

    def download(self, key, url):
        retry = 0

        dt = datetime.datetime.now()
        dirName = dt.strftime('video-riyo-%m%d');
        fullDirPath = os.path.join(UPLOAD_FILE_PATH, dirName)
        if not os.path.isdir(fullDirPath):
            os.makedirs(fullDirPath)

        startBytes = 0
        saveFilePath = os.path.join(fullDirPath, key)

        #文件已存在的话，先删除
        if os.path.exists(saveFilePath):
            os.remove(saveFilePath)

        while True:
            try:
                req = urllib2.Request(url)
                if startBytes != 0:
                    req.add_header("Range", 'bytes={0}-'.format(startBytes))
                res = urllib2.urlopen(req)

                with open(saveFilePath, "ab+") as saveFile:
                    while True:
                        data = res.read(1024*32)
                        length = len(data)
                        if length == 0:
                            self.logger.info("Download video done. key:{0} url:{1}".format(key, url))
                            return

                        saveFile.write(data)
                        startBytes += length

            except Exception as e:
                self.logger.warn("WSVideoRecord download exception. key:{0} url:{1} retry:{2} e:{3}".format(key, url, retry, e))
                retry += 1
                if retry > 5:
                    self.logger.error("WSVideoRecord download retry reach max. key:{0} url:{1}".format(key, url))
                    break

class WSVideoRecordNotifyHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, **kwargs ):
        super(WSVideoRecordNotifyHandler, self).__init__(application, request, **kwargs)
        self.logger = logging.getLogger("rebroadcastall")

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')

    def post(self):
        messageType = self.get_argument('message_type')
        try:
            raw_data = self.request.body
            if messageType.lower() == "ws_record_finish":
                self.handle_data(raw_data)

        except Exception as e:
            self.logger.warn("WSRecordNotifyHandler exception. e:{0}".format(e))

        self.set_status(200)
        self.write("OK")

    def handle_data(self, data):
        jsonData = base64.urlsafe_b64decode(data)
        self.logger.info("WS record message. data:{0}".format(jsonData))

        notifyData = json.loads(jsonData)
        self.start_download(notifyData)

    def get(self):
        global wsRecordDownloadListLock
        global wsRecordDownloadList

        response = dict()
        with wsRecordDownloadListLock:
            for persistentId, thread in wsRecordDownloadList.items():
                response[persistentId] = thread.item

        self.write(json.dumps(response))

    def start_download(self, data):
        for item in data["items"]:
            downloadThread = WSVideoRecordDownloadThread(item)
            with wsRecordDownloadListLock:
                wsRecordDownloadList[item["persistentId"]] = downloadThread
                downloadThread.start()

                self.logger.info("Append download thread. data:{0}".format(item))


class FlvbroadcastThread(threading.Thread):
    '''视频转推直播线程'''
    def __init__(self):
        threading.Thread.__init__(self)
        self.loger=logging.getLogger("rebroadcastall")
        global FFMPEG
        self.ffmpeg = FFMPEG
        global FLVPATH
        self.flvpathsrc = FLVPATH
        global flvlist   #全局任务列表
        self.flvlist = flvlist
        self.runlist = {} #运行数据列表
        self.exit   = False
    #----------------------------------------------------------------------
    def StartCastLive(self, id, flvpath, desaddr):
        """处理开启流推流"""
        try:
            tmp=self.flvlist[id][0].pop(0)
        except IndexError:
            del self.flvlist[id]

        if not os.path.exists(flvpath):
            self.loger.error(u'%s文件不存在' %flvpath)
            if self.flvlist[id][0] == []:
                self.loger.info(u"%s最后一个视频而且文件不存在清空列表" %self.flvlist)
                del self.flvlist[id]
            return

        command =[self.ffmpeg, "-re", "-i", flvpath, "-acodec", "copy", "-vcodec", "copy", "-f", "flv",desaddr]
        p = subprocess.Popen(command)
        
        self.loger.info(u'stop接口开启线程%s,命令%s' %(id,command))
        flvname = flvpath.split('/')[-1]
        timenow = int(time.time())

        statusjson = {"flvname": flvpath, "starttime": timenow, "destaddr": desaddr}

        global runstatus
        runstatus[id] = statusjson
        self.runlist[id] = p

    def StopCastLive(self, mid):
        """处理开启流推流"""
        try:
            self.loger.info("Stop Cast Live Begin: {0}".format(mid))
            p = self.runlist[mid]
            p.terminate()
            p.wait()
            self.loger.info("Stop Cast Live End: {0}".format(mid))
        except Exception as e:
            self.loger.error("Stop Cast Live Exception. {0}".format(e))

    def run(self):
        """执行函数"""
        while True:

            if self.flvlist:
                fl = copy.deepcopy(self.flvlist)
                for n in fl:
                    id = n
                    try:
                        flvname = self.flvlist[id][0][0]
                    except:
                        self.loger.debug(u"播放列表异常%s,%s" % (self.flvlist,id))
                        continue
                    flvpath = os.path.join(self.flvpathsrc, flvname)
                    desaddr = self.flvlist[id][1]
                    if id not in self.runlist:
                        self.StartCastLive(id, flvpath, desaddr)

            if self.runlist:
                runlisttmp = []
                mylock1.acquire()
                for k, v in self.runlist.items():
                    runlisttmp.append(k)
                mylock1.release()

                for m in runlisttmp:
                    if not (m in self.flvlist):
                        self.loger.info(u'%s已经在任务队列删除'%m)
                        mid = m
                        self.StopCastLive(mid)

                    if not (self.runlist[m].poll() is None):
                        del self.runlist[m]
                        try:
                            tmpflv = self.flvlist[m][0]
                        except:
                            self.loger.error(u'列表文件错误%s-%s'%(self.flvlist,m))
                            tmpflv = [11111111111]
                            
                        if tmpflv == []:
                            self.loger.info(u"%s最后一个视频清空列表" %self.flvlist)
                            del self.flvlist[m]
                            try:
                                del runstatus[m]
                            except:
                                self.loger.info(u'%s清除失败 '% runstatus)
            time.sleep(1)

settings = {
    "static_path": STATIC_PATH,
    'static_url_prefix': '/static/',
}

if __name__ == '__main__':
    if not os.path.exists('log'):
        os.mkdir("log")

    logging.config.fileConfig("logging.conf")

    if not platform.system() == "Windows":
        daemonize()

    liveapp = RebroadcastThread()
    liveapp.start()

    flvapp = FlvbroadcastThread()
    flvapp.start()

    # 定时关闭重播
    time_close_room_thread = TimeCloseVideoThread()
    time_close_room_thread.start()

    monitor = UploadFileDirMonitor(UPLOAD_FILE_PATH)
    monitor.start()

    application = tornado.web.Application([
        (r"/start", StartHandler),
        (r"/stop", StopHandler),
        (r"/status", StatusHandler),
        (r"/listfile", ListUploadFilesHandler),
        (r"/wsvideonotify", WSVideoRecordNotifyHandler)
        (r"/timestop", TimeStopHandler)

    ],**settings)

    application.listen(7380)
    tornado.ioloop.IOLoop.instance().start()
