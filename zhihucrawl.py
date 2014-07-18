# coding: utf-8
from pyquery import PyQuery as pq
import urllib, urllib2
import os, time, datetime, random, shutil, threading
import re, string, Queue
import gzip, zlib
import cookielib

class Pack():
    finishedThreadNum = 0

class ImageThread(threading.Thread):
    def __init__(self, lock, threadName, g_queue, p):
        super(ImageThread, self).__init__(name=threadName)  # 注意：一定要显式的调用父类的初始化函数。
        self.lock = lock
        self.queue = g_queue
        self.pack = p
    
    def run(self):
        while True:
            self.lock.acquire()
            if self.queue.qsize() > 0:
                tup = self.queue.get()
                urllib.urlretrieve(tup[0], tup[1])
            if self.pack.finishedThreadNum >= 0:
                self.pack.finishedThreadNum += 1
                self.lock.release()
                break
            self.lock.release()
            time.sleep(1)

class ZHYear():
    def __init__(self, y, m, d):
        self.year = y
        self.month = m
        self.day = d
    
    def newerThan(self, a):
        if self.year - a.year != 0:
            return self.year - a.year
        if self.month - a.month != 0:
            return self.month - a.month
        return self.day - a.day

class ZhihuGet(object):
    # 初始化
    def __init__(self):
        confDict = self.loadConfig()
        self.targetUser = confDict['targetUser']
        self.docRootDir = confDict['docRootDir']
        self.sleepMin = string.atoi(confDict['sleepMin'])
        self.sleepMax = string.atoi(confDict['sleepMax'])
        self.oldLimit = string.atoi(confDict['oldLimit'])
        self.startPage = string.atoi(confDict['startPage'])
        #self.attempTimes = string.atoi(confDict['attempTimes'])
        self.loginUserName = confDict['loginUserName']
        self.loginPassword = confDict['loginPassword']
        self.loginShowName = confDict['loginShowName']
        self.dirSeparator = confDict['dirSeparator']
        self.sysEncoding = confDict['sysEncoding']
        self.downloadImageThread = string.atoi(confDict['downloadImageThread'])
        self.alwaysGetAll = False
        if confDict['alwaysGetAll'].lower() == 'true':
            self.alwaysGetAll = True
        self.debug = False
        if confDict['debug'].lower() == 'true':
            self.debug = True
        self.backup = False
        if confDict['backup'].lower() == 'true':
            self.backup = True
        self.downloadImage = False
        if confDict['downloadImage'].lower() == 'true':
            self.downloadImage = True
        self.backupDir = "{}{}bak{}{}-{}".format(self.docRootDir, self.dirSeparator,
                self.dirSeparator, self.targetUser,
                time.strftime('%Y%m%d-%H%M%S', time.localtime(time.time())))
        self.saveHtmlDir = self.docRootDir + self.dirSeparator + self.targetUser
        self.statusFileName = self.saveHtmlDir + self.dirSeparator + "status"
        self.answerURL = 'http://www.zhihu.com/people/{}/answers'.format(self.targetUser)
        self.hasMeetOld = False
        self.logFileName = "{}{}trace-{}.log".format(self.docRootDir,
                    self.dirSeparator, self.targetUser)
        self.queue = Queue.Queue()
        self.lock = threading.Lock()
        self.pack = Pack()
        self.pack.finishedThreadNum = -1
        
        # 设置cookie
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookielib.CookieJar()));
        urllib2.install_opener(opener);
        
    def isDebug(self):
        return self.debug
    
    # usage 信息
    def usage(self):
        print "PWS$./crawZhihu.py configFilePath (default ./zhihu.conf)"
    
    # 加载参数
    def loadConfig(self, confPath="zhihu.conf"):
        if not os.path.exists(confPath):
            print "configuration file " + confPath + " not found!"
            self.usage()
            return
        fp = open(confPath, "r");
        confDict = {}
        for eachline in fp:
            eachline = eachline.strip()
            if eachline == '' or eachline[0] == '#':
                continue
            strings = eachline.strip().split("=")
            confDict.setdefault(strings[0].strip(), strings[1].strip())
        return confDict
    
    # 打印log信息    
    def logging(self, content, force=False):
        if self.isDebug() or force:
            print content
        logtime = time.strftime('%Y-%m-%d, %H:%M:%S', time.localtime(time.time()))
        file_object = open(self.utf8ToSys(self.logFileName), "a+")
        file_object.write("{}    {}\n".format(logtime, content))
        file_object.close()

    # 查看是否已经有存档
    def prepareDirs(self):
        if os.path.exists(self.utf8ToSys(self.saveHtmlDir)) is False:
            self.logging("dir {} not exists, creat now".format(self.saveHtmlDir), True)
            os.mkdir(self.utf8ToSys(self.saveHtmlDir))
            if os.path.exists(self.utf8ToSys(self.saveHtmlDir)) is False:
                self.logging("creat dir {} failed, exit".format(self.saveHtmlDir), True)
                exit() 
            self.logging("get all answers", True)
            return True
        else:
            self.logging("update new answers", True)
            return False
    
    # 记录最后修改的时间
    def tagLastModifacationToFile(self):
        timenow = time.strftime('%Y-%m-%d-%H-%M', time.localtime(time.time()))
        file_object = open(self.utf8ToSys(self.statusFileName), 'a')
        file_object.write("updated at: " + timenow + "\r\n")
        file_object.close()

    # 检查登陆，并在需要时重新登录
    def checkAndLogin(self):
        if self.hasLogin() is False:
            self.logging("not logged in, try to login", True)
            content = self.login()
            if self.hasLogin(content) is False:
                self.logging("login failed, exit", True)
                return False
        return True

    def buildReq(self, url, postdata=None):
        req = None
        loginHeaders = [("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"),
                        ("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"),
                        ("Accept", "*/*"), ("X-Requested-With", "XMLHttpRequest"),
                        ("Accept-Encoding", "gzip,deflate,sdch"),
                        ("Accept-Language", "en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4"),
                        ("Accept-Charset", "GBK,utf-8;q=0.7,*;q=0.3"),
                        ("Referer", "http://www.zhihu.com")]
        if postdata is None:
            req = urllib2.Request(url)
        else:
            req = urllib2.Request(url, urllib.urlencode(postdata))
        for i in loginHeaders:
            req.add_header(i[0], i[1])
        return req

    # 如果是gzip，则解压缩
    def getResponseContent(self, resp):
        if "{}".format(resp.info()).find("Content-Encoding: gzip") != -1:
            self.logging("page compressed using gzip, decompress it")
            return zlib.decompress(resp.read(), 16 + zlib.MAX_WBITS);
        else:
            return resp.read()

    # 登录函数
    def login(self):
        xsrf = self.get_xsrf()
        postdata = {"email": self.loginUserName,
                    "password": self.loginPassword,
                    '_xsrf': xsrf}
        # login
        self.logging("logging in now, params: {}".format(postdata), True)
        req = self.buildReq("http://www.zhihu.com/login", postdata)
        resp = urllib2.urlopen(req)
        # 抓主页
        req = self.buildReq("http://www.zhihu.com")
        resp = urllib2.urlopen(req)
        return self.getResponseContent(resp)
        
    
    # 获取登陆用的_xsrf
    def get_xsrf(self):
        content = urllib2.urlopen("http://www.zhihu.com").read()
        str1 = content.split('_xsrf')
        str2 = str1[1].split('value="')
        return str2[1].split('"/>')[0]
    
    # 将content保存到文件，注意小心重名的情况
    def saveToFile(self, content, targetFile):
        self.logging("saving file " + targetFile)
        file_object = open(self.utf8ToSys(targetFile), 'w')
        file_object.write(content)
        file_object.close()
    
    def utf8ToSys(self, str):
        return str.decode('UTF-8').encode(self.sysEncoding)
    
    def sysToUTF8(self, str):
        return str.decode(self.sysEncoding).encode("UTF-8")
    
    # 将回答保存到文件，并按照一定格式命名，只有最后一步才需要转换格式，所以这里不必
    def saveAnswerToFile(self, title, vote, date, questionID, answerID, content):
        if 'unicode' in str(type(title)):
            title = title.encode("utf-8")
        fileName = "{}{}[{}]-[{}]-v{}-q{}-a{}.html".\
            format(self.saveHtmlDir, self.dirSeparator, date, title, vote, questionID, answerID)
        self.saveToFile(content, fileName)
    
    # 验证是否已经登陆
    def hasLogin(self, content=None):
        if content is None:
            content = urllib2.urlopen("http://www.zhihu.com").read()
        if content.find(self.loginShowName) != -1 and content.find("我的草稿") != -1:
            return True
        else:
            return False
    
    # 计算最大页码
    def getMaxPageNumber(self, d):
        pages = d('.zm-invite-pager').text().split(' ')
        largestValue = 1
        for page in pages:
            try:
                largestValue = string.atoi(page)
            except ValueError:
                pass
        return largestValue
    
    # 获得文件夹下，最近的回答的文件名
    def getLatestAnswerFileName(self):
        self.logging("try to get the latest answer")
        # 按照系统的去查找，但是一旦找到，就转成UTF-8
        files = os.listdir(self.utf8ToSys(self.saveHtmlDir))
        pattern = re.compile('^\[(\d{4})-(\d{2})-(\d{2})\].*html$')
        latestZHYear = ZHYear(0, 0, 0)
        latestFileName = None
        for file in files:
            # 按照系统的去查找，但是一旦找到，就转成UTF-8
            file = self.sysToUTF8(file)
            match = pattern.findall(file)
            for pp in match:
                curZHYear = ZHYear(string.atoi(pp[0]), string.atoi(pp[1]), string.atoi(pp[2]))
                if curZHYear.newerThan(latestZHYear) >= 0:
                    latestZHYear = curZHYear
                    latestFileName = file
        if latestFileName is not None:
            self.logging("the latest answer is " + latestFileName)
        else:
            self.logging("no previous answer at all")
        return latestFileName
    
    # 通过questionID, answerID匹配文件名
    def getFileNameByQAndA(self, questionID, answerID):
        files = os.listdir(self.utf8ToSys(self.saveHtmlDir))
        pattern = re.compile('.*-q{}-a{}.html$'.format(questionID, answerID))
        for file in files:
            file = self.sysToUTF8(file)
            match = pattern.findall(file)
            for pp in match:
                return pp
        return None
    
    # 计算时间，要注意日期转换
    def transToCommonDate(self, dateStr):
        pattern = re.compile('^(\d{4}-\d{2}-\d{2})$')
        match = pattern.findall(dateStr)
        if match:
            return dateStr
        pattern = re.compile('.*昨天\s\d{1,2}:\d{1,2}')
        match = pattern.findall(dateStr)
        if match:
            yestoday = datetime.date.today() - datetime.timedelta(days=1)
            return yestoday
        pattern = re.compile('^\d{1,2}:\d{1,2}$')
        match = pattern.findall(dateStr)
        if match:
            return datetime.date.today()
        return "2000-01-01"

    # 备份文件
    def backupFiles(self):
        self.logging("backup dir " + self.saveHtmlDir + 
                     " -->> " + self.backupDir, True)
        shutil.copytree(self.utf8ToSys(self.saveHtmlDir), self.utf8ToSys(self.backupDir))
    
    # 启动线程
    def startThread(self):
        self.logging("download image, using {} threads".format(self.downloadImageThread))
        for i in range(self.downloadImageThread): 
            ImageThread(self.lock, "thread-" + str(i), self.queue, self.pack).start()
    
    # 检查队列为空，连续5次，即认为结束
    def waitForThread(self):
        self.logging("waitting for thread to finish", True)
        waitRound = 5
        while waitRound > 0:
            self.lock.acquire()
            if self.queue.qsize() == 0:
                waitRound -= 1
            self.lock.release()
            time.sleep(1)
        # 通知线程可以退出了
        self.lock.acquire()
        self.pack.finishedThreadNum = 0
        self.lock.release()
        self.logging("waiting for thread finish", True)
        while True:
            self.lock.acquire()
            if self.pack.finishedThreadNum >= self.downloadImageThread:
                self.lock.release()
                break
            self.lock.release()
            time.sleep(1)
        self.logging("main thread finish", True)
            
    # 工作主函数
    def work(self,):
        self.loadConfig()
        # 首先判断是否已经登录，如果登录失败，则退出
        loginStatus = self.checkAndLogin()
        if not loginStatus:
            self.logging("login failed, quit")
            return
        if self.downloadImage:
            self.startThread()
        # 第一次存档为True，增量为False
        firstTime = self.prepareDirs()
        if not firstTime and self.backup:
            self.backupFiles()
        if self.alwaysGetAll:
            firstTime = True
        self.getUserAnswers(firstTime)
        if self.downloadImage:
            self.waitForThread()
        self.tagLastModifacationToFile()
    
    # 获取用户的回答
    def getUserAnswers(self, all):
        # 获取最新的文件的qID和aID
        latestFile = self.getLatestAnswerFileName()
        latestQID = 0
        latestAID = 0
        if latestFile is None:  # 没有符合格式的文件，需要全抓
            all = True
        else:  # 计算出最新的questionID和answerID
            pattern = re.compile('^\[\d{4}-\d{2}-\d{2}\].*-q(\d{1,50})-a(\d{1,50}).html$')
            match = pattern.findall(latestFile)
            for pp in match:
                latestQID = pp[0]
                latestAID = pp[1]
        # 默认是要抓第一页的，顺便计算回答的总页数
        pageContent = urllib2.urlopen("{}?page={}".
                                          format(self.answerURL, self.startPage)).read()
        d = pq(pageContent)
        pageMax = self.getMaxPageNumber(d)
        currentPage = self.startPage
        ret = False
        while True:
            self.logging("parsing page {} of {}".format(currentPage, pageMax), True)
            # 如果不是需要全部抓取，那么看看现在抓够了没有
            # 遇到老答案之后，再向前寻找10个老答案，并更新
            ret = self.parseAnswerAndSave(d, latestQID, latestAID, all)
            if not all and ret:  # 不用全抓，而且发现了重复 
                return
            if currentPage >= pageMax:  # 已经是最后一页
                break
            # 计算下一页的pq值
            currentPage += 1
            pageContent = urllib2.urlopen("{}?page={}".
                                          format(self.answerURL, currentPage)).read()
            d = pq(pageContent)
        
    # 用@替换可能的特殊字符
    def transTitle(self, title):
        target = r'<>/\|:"*,?\''
        for i in target:
            title = title.replace(i, '@')
        # 万一注入攻击了呢……
        # 这是一种逗逼行为
        title = title.replace('/', ' OR ').replace('sudo', 'SUDO').\
                replace('rm', 'RM').replace('mv', 'MV')   
        return title
        
    # 将d中对应的每个答案都copy到一个html中
    def parseAnswerAndSave(self, d, latestQID, latestAID, getAll):
        zmAll = d('#zh-profile-answer-list')('.zm-item')
        self.logging("parsing answers, there are {} answers in this page".format(len(zmAll)))
        for zm in zmAll:
            ele = pq(zm)
            title = self.transTitle(ele(".question_link").html())
            vote = ele(".zm-item-vote-count").html()
            # date有多种格式，比如2014-01-01, 昨天15:30, 11:15等 
            date = ele(".zm-item-rich-text")(".answer-date-link").text()
            dateAll = date.split(" ")
             date = self.transToCommonDate("{}".format(dateAll[len(dateAll)-1]))
            # 不论答案如何，把url全拉下来去请求
            answerURL = "http://www.zhihu.com" + ele(".question_link").attr('href')
            pattern = re.compile(r'/question/(\d{1,50})/answer/(\d{1,50})')
            match = pattern.findall(answerURL)
            for pp in match:
                questionID = pp[0]
                answerID = pp[1]
            self.logging("trace answer: " + answerURL)
            content_stream = urllib2.urlopen(answerURL)
            pageContent = content_stream.read()
            # 不用下载图片的话，就不需要调用这个函数了
            if self.downloadImage:
                pageContent = self.downloadImageAndReplace(pageContent)
            # 判断是否已经有本地存档。要全抓取的时候latest=0，不会出现误判，然后遍历oldLimit个答案并覆盖
            if not getAll and latestQID == questionID and latestAID == answerID:
                self.logging("meet old answer, will parse {} more answers".
                             format(self.oldLimit), True)
                self.hasMeetOld = True
            # 先删除老文件，防止匹配把新文件删了
            if self.hasMeetOld or getAll:
                self.oldLimit -= 1
                oldFileName = self.getFileNameByQAndA(questionID, answerID)
                if oldFileName is not None:
                    self.logging("covering " + oldFileName, True)
                    # 这个是直接的文件操作
                    os.remove(self.utf8ToSys(self.saveHtmlDir + self.dirSeparator + oldFileName))
            self.saveAnswerToFile(title, vote, date, questionID, answerID,
                                  pageContent.strip())
            if not getAll and self.oldLimit <= 0:
                self.logging("enough old files, stop parsing", True)
                return True
            # sleep一个随机时间
            timeToSleep = random.uniform(self.sleepMin, self.sleepMax)
            if 'unicode' in str(type(title)):
                title = title.encode("utf-8")
            self.logging("save answer of {} finished, sleep {} seconds for next request".\
                    format(title, timeToSleep))
            time.sleep(timeToSleep)
        return False
    
    # 把要下载的源地址和存储路径存到队列中
    def putIntoQueue(self, src, des):
        self.lock.acquire()
        # 路径转成系统encoding
        tup = (src, self.utf8ToSys(des))
        self.queue.put(tup)
        self.logging("queue length {} aftre put {} into queue".
                     format(self.queue.qsize(), tup))
        self.lock.release()

    # 把绝对路径替换为相对路径，同时下载图片
    def downloadImageAndReplace(self, content):
        pattern = re.compile('<img src="//(s\d.zhimg.com/misc/whitedot.jpg)(".{1,500})data-actualsrc="http://(pic\d.zhimg.com/\w{1,50}.jpg)(".{0,300})>')
        match = pattern.findall(content)
        for line in match:
            pairs = line[2].split('/')
            dir = pairs[0]
            name = pairs[1]
            if not os.path.exists(self.saveHtmlDir + self.dirSeparator + dir):
                os.mkdir(self.saveHtmlDir + self.dirSeparator + dir)
            # 放进队列
            self.putIntoQueue("http://" + line[2],
                 self.saveHtmlDir + self.dirSeparator + dir + self.dirSeparator + name)
            # 这是要把原网页中，图片的位置全部替换为本地的文件位置
            rawString = '<img src="//' + line[0] + line[1] + 'data-actualsrc="http://' + line[2] + line[3] 
            #newString = '<img src="' + self.saveHtmlDir + self.dirSeparator + line[2] + line[1] + 'data-actualsrc="' + self.saveHtmlDir + self.dirSeparator + line[2] + line[3]
            newString = '<img src="'+ line[2] + line[1] + 'data-actualsrc="' + self.saveHtmlDir + self.dirSeparator + line[2] + line[3]
            #self.logging("replace {} -->> {}".format(rawString, newString))
            content = content.replace(rawString, newString)
        pattern = re.compile('http://(pic\d.zhimg.com/\w{1,50}.jpg)')
        match = pattern.findall(content)
        for line in match:
            pairs = line.split('/')
            if not os.path.exists(self.utf8ToSys(self.saveHtmlDir + self.dirSeparator + pairs[0])):
                os.mkdir(self.utf8ToSys(self.saveHtmlDir + self.dirSeparator + pairs[0]))
            self.putIntoQueue("http://" + pairs[0] + "/" + pairs[1],
                               self.saveHtmlDir + self.dirSeparator + pairs[0] + self.dirSeparator + pairs[1])
            content = content.replace("http://" + pairs[0] + "/" + pairs[1],
                    pairs[0] + self.dirSeparator + pairs[1])
            #self.logging("replace {} -->> {}".format("http://" + pairs[0] + "/" + pairs[1], pairs[0] + self.dirSeparator + pairs[1]))
        return content
        
if __name__ == '__main__':
    z = ZhihuGet();
    z.work()
