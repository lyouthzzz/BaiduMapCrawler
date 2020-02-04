'use strict'
const puppeteer = require('puppeteer');
const { PuppeteerDownloader } = require('./PuppeteerDownloader')
const { EventEmitter } = require('events');
const { MongoClient } = require('mongodb');
var fs = require('fs')
var sleep = require('sleep');

var cookieCollection = []

function setCookies(cookies) {
    let newCookieCollection = []
    if (Array.isArray(cookies)) {
        for (let cookie of cookies) {
            newCookieCollection.push(cookie)
        }
    } else {}
    cookieCollection = newCookieCollection
}

function getCookie() {
    return cookieCollection
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve(), ms))
}

class Schedule {

    constructor() {
        this.queue = []
    }

    push(urls) {
        if (Array.isArray(urls)) {
            urls.forEach(url => {
                this.queue.push(url)
            })
        } else {
            this.queue.push(urls)
        }
    }

    pop() {
        return this.queue.shift()
    }
}

class DataPipeline {

    constructor() {}

    async connectMongo() {
        let mongoUri = 'mongodb://localhost:27017'
        try {
            this.mongoClient = await MongoClient.connect(mongoUri)
            if (this.mongoClient.isConnected()) {}
            this.mapConnection = this.mongoClient.db('tianrang').collection('CheLiangDengJiJiGuan')
            return true
        } catch (e) {
            console.log(e)
        }
        return false
    }

    async closeConnect() {
        await this.mongoClient.close()
    }

    async insert(items) {
        try {
            if (Array.isArray(items)) {
                items.forEach(async(item) => {
                    try {
                        await this.mapConnection.insert(item)
                        console.log(`insert successful ${JSON.stringify(item)}`)
                    } catch (e) {
                        console.log(e)
                    }
                })
            } else {
                try {
                    await this.mapConnection.insert(items)
                    console.log(`insert successful ${JSON.stringify(items)}`)
                } catch (e) {
                    console.log(e)
                }
            }
        } catch (e) {
            console.log(e)
        }
        return true
    }

}

class Site {
    constructor() {
        this.cookieCollection = []
    }
    setCookies(cookies) {
        let newCookieCollection = []
        if (Array.isArray(cookies)) {
            for (let cookie of cookies) {
                newCookieCollection.push(cookie)
            }
        } else {}
        this.cookieCollection = newCookieCollection
    }

    getCookie() {
        return this.cookieCollection
    }
}

class BaiduDituCrawler extends EventEmitter {

    constructor() {
        super()
    }

    async init() {
        this.schedule = new Schedule()
        this.dataPipeline = new DataPipeline()
        this.site = new Site()
        await this.dataPipeline.connectMongo()

        this.browser = await puppeteer.launch({
                headless: true,
                "env": {
                    "DEBUG": "puppeteer:*,-puppeteer:protocol"
                }
            })
            // this.page = await this.browser.newPage()
    }

    async doRequest(url, options) {
        return new Promise(async(resove, reject) => {
            // let transformedOptions = await this.transformOptions(options)
            let timeoutFunc = setTimeout(async() => {
                await page.close()
                resove(new Error('timeout'))
            }, 10 * 1000)
            let page = await this.browser.newPage()
            if (options.cookies.length > 0) {
                await page.setCookie(...options.cookies)
            }
            await page.setRequestInterception(true)
            await page.on('request', request => request.continue())
            page.on('requestfinished', async(response) => {
                await options.requestCallBack(response, page, timeoutFunc, resove, reject)
            })
            page.exposeFunction('saveCookie', setCookies)
            await page.goto(url)
            await page.close()
            clearTimeout(timeoutFunc)
            resove(new Error('return error'))
        })

    }

    async requestCallBack(request, page, timeoutFunc, resove, reject) {
        if (/map.baidu.com\/\?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct.*from=webmap&wd=.*pn=/.test(request.url())) {
            try {
                let response = await request.response()
                let responseJson = await response.json()
                    // collect cookies
                let cookies = await page.cookies()
                    // save cookie to site
                try {
                    console.log(cookieCollection)
                    await page.evaluate(async(cookies) => {
                        await window.saveCookie(cookies)
                    }, cookies)
                } catch (e) {
                    console.log(e)
                }

                clearTimeout(timeoutFunc)
                await page.close()
                resove(responseJson)
            } catch (e) {
                reject(e)
            }
        } else {
            request.continue()
        }
    }

    formatUrl(location, pageNum) {
        return `https://map.baidu.com/search/%E8%BD%A6%E8%BE%86%E7%AE%A1%E7%90%86/@${location}?querytype=s&from=webmap&wd=%E8%BD%A6%E8%BE%86%E7%AE%A1%E7%90%86&pn=${pageNum-1}&nn=${(pageNum-1)*10}`
    }

    async addUrl(url) {
        this.schedule.push(url)
    }

    async download(url) {
        console.log(`Download ${url} start`)
        let matcher = url.match(/.*@(.*)\?.*pn=(\d+)/)
        let location = matcher[1]
        let pageNum = parseInt(matcher[2])

        let urls = []
        let items = []
        let options = {
            requestCallBack: this.requestCallBack,
            cookies: getCookie()
        }

        let resonseData = await this.doRequest(url, options)
        if (resonseData instanceof Error) {
            console.log(resonseData)
            urls.push(url)
            return { urls, items }
        }
        let totalNum = resonseData['result']['aladdin_res_num']
        if (pageNum === 0) {
            for (let i = 2; i <= Math.ceil(totalNum / 10); i++) {
                urls.push(this.formatUrl(location, i))
            }
        }
        resonseData['content'].forEach(item => {
                delete item.ext_display
                delete item.ext
                item.url = url
                items.push(item)
            })
            // await this.puppeteerDownloader.close()
        console.log(`Download ${url} end`)
        return { urls, items }
    }

    async close() {
        await this.dataPipeline.closeConnect()
        await this.browser.close()
    }

    async run() {
        while (true) {
            try {
                let url = await this.schedule.pop()
                if (url) {
                    let result = await this.download(url)
                    let urls = result['urls']
                    let items = result['items']
                    await this.dataPipeline.insert(items)
                    await this.schedule.push(urls)
                } else {
                    await this.close()
                    console.log('Crawler end !')
                    break
                }
                sleep.msleep(1 * 1000)
            } catch (e) {
                console.log(e)
            }

        }

    }
}

(async function() {
    // let location = '13057066.72,4032808.36'
    let baiduDituCrawler = new BaiduDituCrawler({});
    await baiduDituCrawler.init()
    let data = fs.readFileSync('city_geo.txt', 'utf-8')
    data.split('\n').forEach(async city => {
            let location = city.split(' ')[1]
            await baiduDituCrawler.addUrl(baiduDituCrawler.formatUrl(location, 1))
        })
        // await baiduDituCrawler.addUrl(baiduDituCrawler.formatUrl(location, 1))
    await baiduDituCrawler.run()
})()
