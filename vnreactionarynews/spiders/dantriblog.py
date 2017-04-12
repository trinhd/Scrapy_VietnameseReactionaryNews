# -*- coding: utf-8 -*-
import scrapy
import pymongo
import re

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request
from vnreactionarynews.items import VNReactionaryNewsItem

from pymongo import MongoClient


class DantriblogSpider(scrapy.Spider):
	name = "dantriblog"
	allowed_domains = ["dantri.com.vn/blog/"]
	start_urls = (
		'http://dantri.com.vn/blog.htm',
		#'http://dantriblog.blogspot.com/2017/04/vi-sao-cong-san-oi-cam-vinh-vien-ban.html',
		#'http://dantriblog.blogspot.com/2017/04/sai-gon-tiep-tuc-anh-du-kich-hit-and.html',
		#'http://dantriblog.blogspot.com/2017/04/viet-nam-trong-bong-toi-nhung-khong-bi.html',
	)

	"""rules = (
		Rule(LinkExtractor(deny=(
			'.*\/cong\-dong\/hoi\-dap\/.*',
			'.*\/tin\-tuc\/cong\-dong\/.*',
			'.*\/tin\-tuc\/tam\-su\/.*',
			'.*\/tin\-tuc\/cuoi\/.*',
		), deny_domains=(
			'.*video\.vnexpress\.net.*',
			'.*ione\.vnexpress\.net.*',
			'.*raovat\.vnexpress\.net.*',
		))),
	)"""

	#danh sách các pattern để lọc link
	denyLinks = [
		#re.compile(r'.*\/cong\-dong\/hoi\-dap\/.*'),
		#re.compile(r'.*\/tin\-tuc\/cong\-dong\/.*'),
		#re.compile(r'.*\/tin\-tuc\/tam\-su\/.*'),
		#re.compile(r'.*\/tin\-tuc\/cuoi\/.*'),
		#re.compile(r'.*video\.vnexpress\.net.*'),
		#re.compile(r'.*ione\.vnexpress\.net.*'),
		#re.compile(r'.*raovat\.vnexpress\.net.*'),
		re.compile(r'https:\/\/dantri\.com\.vn.*'),
	]
	allowLinks = [
		#re.compile(r'.*\/tin\-tuc\/giao\-duc.*'),
		re.compile(r'.*dantri\.com\.vn\/blog\/.*'),
	]

	count = 0
	crawledLinks = []
	linkSuffix = ".htm"

	def parse(self, response):
		links = response.xpath("//a/@href").extract()
		#links = ()

		client = MongoClient()
		db = client.dantriblog
		collCrawledLinks = db.crawledLinks
		if len(self.crawledLinks) == 0:
			for cl in collCrawledLinks.find():
				self.crawledLinks.append(str(cl["crawled"])) #doc lai tu csdl nhung link da crawl
			self.count = db.all.find().count()

		linkPattern = re.compile("^(?:ftp|http|https):\/\/(?:[\w\.\-\+]+:{0,1}[\w\.\-\+]*@)?(?:[a-z0-9\-\.]+)(?::[0-9]+)?(?:\/|\/(?:[\w#!:\.\?\+=&amp;%@!\-\/\(\)]+)|\?(?:[\w#!:\.\?\+=&amp;%@!\-\/\(\)]+))?$")

		for link in links:
			if linkPattern.match(link) and self.linkFilter(link) and not self.linkCrawled(link):
				collCrawledLinks.insert_one({"crawled": link})
				self.crawledLinks.append(link)
				yield Request(link, self.parse)
		
		shortIntro = response.xpath('//h2[@class="blogsapo"]/text()').extract_first()
		contents = response.xpath('//div[@id="divNewsContent"]/p/text()').extract()
		title = response.xpath('//a[@id="ctl00_IDContent_BlogDetail1_hplTitle"]/text()').extract_first()
		author = response.xpath('//p[@style="text-align: right;"]/strong/text()').extract_first()
		created = response.xpath('//div[@class="blg-date"]/text()').extract_first()
		
		#print ('Title: ' + title + '\n').encode('utf-8')
		#print ('Author: ' + author + '\n').encode('utf-8')
		#print ('Created: ' + created + '\n').encode('utf-8')
		#print 'Len(contents): ' + str(len(contents)) + '\n'

		if shortIntro == None:
			shortIntro = ""

		if title == None:
			return

		if len(title) <= 2:
			return

		if author == None:
			return

		if created == None:
			return

		if len(contents) == 0:
			return

		document = shortIntro

		"""fFirstPara = False

		for para in contents:
			#print para.encode('utf-8')
			if author in para:
				if fFirstPara == False:
					fFirstPara = True
					#print para.encode('utf-8')
					#print para.find(author)
					#print len(author)
					para = para[para.find("-", para.find(author) + len(author)) + 1 : len(para) - 6]
					index = para.rfind(">", 0, 15)
					if index != -1:
						para = para[index + 1:]
					#print para.encode('utf-8')
			if fFirstPara:
				deTag = self.detectTag(para, 0)
				while deTag[2] < len(para):
					if deTag[0] != '-1' and deTag[1] != '-1':
						para = para.replace(deTag[0], '', 1).replace(deTag[1], '', 1)
					deTag = self.detectTag(para, deTag[2])
				document = document + " " + para

		for i in range(1, len(contents) - 1):
			deTag = self.detectTag(contents[i], 0)
			while deTag[2] < len(contents[i]):
				if deTag[0] != '-1' and deTag[1] != '-1':
					contents[i] = contents[i].replace(deTag[0], '', 1).replace(deTag[1], '', 1)
				deTag = self.detectTag(contents[i], deTag[2])
			document = document + " " + contents[i]"""
		
		#document = document.replace(author, "", 1)

		for para in contents:
			deTag = self.detectTag(para, 0)
			while deTag[2] < len(para):
				if deTag[0] != '-1' and deTag[1] != '-1':
					para = para.replace(deTag[0], '', 1).replace(deTag[1], '', 1)
				deTag = self.detectTag(para, deTag[2])
			document = document + " " + para

		content = self.removeHTMLSpecialEntities(document)

		"""collAll = db.all
		oneRow = {
			"subject": subject,
			"link": response.url,
			"title": title,
			"content": content
		}
		collAll.insert_one(oneRow)"""

		item = VNReactionaryNewsItem()
		item['link'] = response.url
		item['title'] = title
		item['content'] = content
		item['created'] = created
		item['author'] = author
		self.count = self.count + 1
		print self.count
		yield item

	def detectTag(self, sInput, iBegin):
	#Tim dau < dau tien de bat dau xac dinh the tag
		iBeginAngleBracketOpen = sInput.find('<', iBegin)
		if iBeginAngleBracketOpen == -1:
			return ['-1', '-1', len(sInput)] #Da het dau mo tag

		if iBeginAngleBracketOpen < len(sInput): #Neu dau mo the tag khong nam cuoi cung cua doan
			#Xac dinh vi tri khoang trang de xac dinh ten the tag la gi
			iFirstSpaceAfterAngle = sInput.find(' ', iBeginAngleBracketOpen)

			#Xac dinh vi tri dau dong ngoac cua the tag
			iBeginAngleBracketClose = sInput.find('>', iBeginAngleBracketOpen)
			if iBeginAngleBracketClose == -1:
				return ['-1', '-1', iBeginAngleBracketOpen + 1] #Neu khong co dau dong the tag thi do khong phai la tag html, tra ve vi tri tim kiem tiep theo la tu sau dau mo the tag vua tim duoc.

			
			if iBeginAngleBracketClose > iFirstSpaceAfterAngle and iFirstSpaceAfterAngle != -1: #Neu co khoang trang truoc khi co dau dong ngoac the tag, tuc la the tag co attrib
				sTag = sInput[iBeginAngleBracketOpen + 1:iFirstSpaceAfterAngle]
			else: #Nguoc lai, la mot the tag binh thuong
				sTag = sInput[iBeginAngleBracketOpen + 1:iBeginAngleBracketClose]

			#print "Tag: " + sTag
			iEndAngleBracketOpen = sInput.find('</'+sTag+'>', iBeginAngleBracketClose)
			if iEndAngleBracketOpen == -1:
				return ['-1','-1', iBeginAngleBracketClose + 1]

			return [sInput[iBeginAngleBracketOpen: iBeginAngleBracketClose + 1], sInput[iEndAngleBracketOpen: iEndAngleBracketOpen + len(sTag) + 3], iBeginAngleBracketOpen] #Cong 3 do: 1 dau /, 1 dau > va 1 index cach ra
		else:
			return ['-1', '-1', len(sInput)] #Da het cach tim kiem

	def removeHTMLSpecialEntities(self, sInput):
		sOutput = sInput.replace("<br>\n", "\n") #Thay the tag break line
		sOutput = sOutput.replace("<br>", "\n") #Thay the tag break line
		sOutput = re.sub(r'<img\s[\w=\"\-\s\.]{1,}src="http?:\/\/[\w\.\d\/\-]{1,}">', "",sOutput) #Thay the tag img
		sOutput = re.sub(r'&(aacute|Aacute|Acirc|acirc|acute|aelig|AElig|Agrave|agrave|alpha|Alpha|amp|and|ang|Aring|aring|asymp|Atilde|atilde|Auml|auml|bdquo|beta|Beta|brvbar|bull|cap|Ccedil|ccedil|cedil|cent|circ|clubs|cong|copy|crarr|cup|curren|Chi|chi|Dagger|dagger|darr|deg|delta|Delta|diams|divide|Eacute|eacute|Ecirc|ecirc|Egrave|egrave|empty|emsp|ensp|epsilon|Epsilon|equiv|eta|Eta|eth|ETH|euml|Euml|euro|exist|fnof|forall|frac12|frac14|frac34|gamma|Gamma|ge|gt|harr|hearts|hellip|Iacute|iacute|Icirc|icirc|iexcl|igrave|Igrave|infin|int|iota|Iota|iquest|isin|iuml|Iuml|kappa|Kappa|lambda|Lambda|laquo|larr|lceil|ldquo|le|lfloor|lowast|loz|lrm|lsaquo|lsquo|lt|macr|mdash|micro|minus|Mu|mu|nabla|nbsp|ndash|ne|ni|not|notin|nsub|ntilde|Ntilde|nu|Nu|oacute|Oacute|ocirc|Ocirc|oelig|OElig|ograve|Ograve|oline|Omega|omega|Omicron|omicron|oplus|or|ordf|ordm|oslash|Oslash|otilde|Otilde|otimes|Ouml|ouml|para|part|permil|perp|Pi|pi|piv|plusmn|pound|Prime|prime|prod|prop|Psi|psi|phi|Phi|radic|raquo|rarr|rceil|rdquo|reg|rfloor|rho|Rho|rlm|rsaquo|rsquo|sbquo|scaron|Scaron|sdot|sect|shy|Sigma|sigma|sigmaf|sim|spades|sub|sube|sum|sup|sup1|sup2|sup3|supe|szlig|tau|Tau|tilde|times|there4|Theta|theta|thetasym|thinsp|thorn|THORN|trade|uacute|Uacute|uarr|Ucirc|ucirc|Ugrave|ugrave|uml|upsih|upsilon|Upsilon|Uuml|uuml|Xi|xi|yacute|Yacute|yen|yuml|Yuml|Zeta|zeta|zwj|zwnj|;)+', "", sOutput)
		return sOutput

	def linkFilter(self, url):
		print url+'\n'
		if len(self.allowLinks) != 0:
			for allow in self.allowLinks:
				if allow.search(url) != None:
					for deny in self.denyLinks:
						if deny.search(url) != None:
							print 'Khong chap nhan link\n'
							return False
					print 'Chap nhan link\n'
					return True
			print 'Khong chap nhan link\n'
			return False
		for deny in self.denyLinks:
			if deny.search(url) != None:
				print 'Khong chap nhan link\n'
				return False
		
		print 'Chap nhan link\n'
		return True

	def linkCrawled(self, url):
		if self.linkSuffix != "":
			originUrl = url[:url.find(self.linkSuffix) + len(self.linkSuffix)]
			for link in self.crawledLinks:
				if link.find(originUrl) != -1:
					return True
		else
			return (url in self.crawledLinks)
		return False

	def detectEnglish(self, sContent):
		frequentWords = ['the', 'to', 'of', 'and', 'in', 'that', 'have', 'it', 'for', 'not', 'on', 'with']
		frequent = 3
		count = 0
		for word in frequentWords:
			if word in sContent.contains:
				count = count + 1
				if count >= frequent:
					return True
		return False