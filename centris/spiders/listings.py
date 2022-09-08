import scrapy
from scrapy import Selector
from scrapy_splash import SplashRequest
import json


class ListingsSpider(scrapy.Spider):
    name = 'listings'
    allowed_domains = ['www.centris.ca']  
    # for aquarium
    http_user = 'user'
    http_pass = 'userpass'
    # скрипт для splash
    script = '''
    function main(splash, args)
        splash:on_request(function(request)
            if request.url:find('css') then
                request.abort()
            end
        end)
      splash.images_enabled = false
      splash.js_enabled = false
      assert(splash:go(args.url))
      assert(splash:wait(0.5))
      return splash:html()
    end
    '''
    # с какой позиции начинаем, нужна для работы с множеством страниц
    position = {
        'startPosition': 0
    }
    #в ответ на этот запрос получаем уникальный ключ для дальнейшей отправки запросов
    def start_requests(self):
        yield scrapy.Request(
            url='https://www.centris.ca/UserContext/Lock',
            method='POST',
            headers={
                'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
                'x-requested-with': 'XMLHttpRequest',
                'content-type': 'application/json'
            },
            body=json.dumps({'uc': 0}),
            callback=self.generate_uck
        )

    # На данном сайте используется уникальный ключ (uck) для каждой сессии, без которого мы не можем получить
    # данные POST-запросом
    # Для корректной работы нужно отправить словарь query с нужными нам фильтрами и получить уникальный
    # ключ для нашей сессии (uck)
    def generate_uck(self, response):
        uck = response.body
        query = {
            "query": {
                "UseGeographyShapes": 0,
                "Filters": [
                    {
                        "MatchType": "GeographicArea",
                        "Text": "Montréal (Island)",
                        "Id": "GSGS4621"
                    }
                ],
                "FieldsValues": [
                    {
                        "fieldId": "GeographicArea",
                        "value": "GSGS4621",
                        "fieldConditionId": "",
                        "valueConditionId": ""
                    },
                    {
                        "fieldId": "Category",
                        "value": "Residential",
                        "fieldConditionId": "",
                        "valueConditionId": ""
                    },
                    {
                        "fieldId": "SellingType",
                        "value": "Rent",
                        "fieldConditionId": "",
                        "valueConditionId": ""
                    },
                    {
                        "fieldId": "LandArea",
                        "value": "SquareFeet",
                        "fieldConditionId": "IsLandArea",
                        "valueConditionId": ""
                    },
                    {
                        "fieldId": "RentPrice",
                        "value": 0,
                        "fieldConditionId": "ForRent",
                        "valueConditionId": ""
                    },
                    {
                        "fieldId": "RentPrice",
                        "value": 1000,
                        "fieldConditionId": "ForRent",
                        "valueConditionId": ""
                    }
                ]
            },
            "isHomePage": True
        }
        # Отправляекм запрос на получение данных с фильтрами, указанными в query с уникальным ключем в headers
        yield scrapy.Request(url='https://www.centris.ca/property/UpdateQuery',
                             method='POST',
                             body=json.dumps(query),
                             headers={'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
                                      'Content-Type': 'application/json; charset=UTF-8',
                                      'referer': 'https://www.centris.ca/en',
                                      # заголовки с 'x-' необходимы, чтобы наш запрос отработал. Без них ошибка
                                      'x-centris-uc': 0,
                                      'x-centris-uck': uck,
                                      'x-requested-with': 'XMLHttpRequest',
                                      },
                             callback=self.update_query,
                             )

    # указываем стартовую позицию и получаем данные
    def update_query(self, response):
        yield scrapy.Request(
            url='https://www.centris.ca/Property/GetInscriptions',
            method='POST',
            body=json.dumps(self.position),
            headers={'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
                     'Content-Type': 'application/json',
                     # заголовки с 'x-' необходимы, чтобы наш запрос отработал. Без них ошибка
                     'x-content-type-options': 'nosniff',
                     'x-permitted-cross-domain-policies': 'none',
                     'x-xss-protection': '1; mode=block'},
            callback=self.parse,
        )

    # парсим данные с главной страницы, далее заходим с помощью Splash на каждую,
    # чтобы достать дополнительные данные
    def parse(self, response):
        resp_dict = json.loads(response.body)
        html = resp_dict.get('d').get('Result').get('html')
        sel = Selector(text=html)
        listings = sel.xpath("//div[@class='property-thumbnail-item thumbnailItem col-12 col-sm-6 col-md-4 col-lg-3']")
        for listing in listings:
            category = listing.xpath("normalize-space(.//span[@class='category']/div/text())").get()
            street = listing.xpath(".//span[@class='address']/div[1]/text()").get()
            region = listing.xpath(".//span[@class='address']/div[2]/text()").get()
            city = listing.xpath(".//span[@class='address']/div[3]/text()").get()
            price = listing.xpath(".//div[@class='price']/span[1]/text()").get()
            url = listing.xpath(".//div[@class='thumbnail property-thumbnail-feature']/a/@href").get()
            abs_url = f'https://www.centris.ca{url}'
            yield SplashRequest(
                url=abs_url,
                # execute - выполнить код из args
                endpoint='execute',
                callback=self.parse_summary,
                # в args передаем скрипт для выполнения в splash
                args={
                    'lua_source': self.script
                },
                # для передачи этих данных в parse_summary помещаем их в мета
                meta={
                    'category': category,
                    'street': street,
                    'region': region,
                    'city': city,
                    'price': price,
                    'url': abs_url,
                }
            )
        # достаем текущее значение счётчика и колличество карточек на одной странице
        count = resp_dict.get('d').get('Result').get('count')
        increment_number = resp_dict.get('d').get('Result').get('inscNumberPerPage')
        if self.position['startPosition'] <= count:
            self.position['startPosition'] += increment_number
            yield scrapy.Request(
                url='https://www.centris.ca/Property/GetInscriptions',
                method='POST',
                body=json.dumps(self.position),
                headers={'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
                         'Content-Type': 'application/json',
                         'x-content-type-options': 'nosniff',
                         'x-permitted-cross-domain-policies': 'none',
                         'x-xss-protection': '1; mode=block'},
                callback=self.parse
            )

    # здесь достаем нужные данные из мета и формируем yield
    def parse_summary(self, response):
        broker_phone = response.xpath("normalize-space(//a[@itemprop='telephone'][1]/text())").get()
        broker_name = response.xpath("(//h1[@class='broker-info__broker-title h5 mb-0'])[1]/text()").get()
        category = response.request.meta['category']
        street = response.request.meta['street']
        region = response.request.meta['region']
        city = response.request.meta['city']
        price = response.request.meta['price']
        url = response.request.meta['url']
        yield {
            'category': category,
            'street': street,
            'region': region,
            'city': city,
            'price': price,
            'url': url,
            'broker_phone': broker_phone,
            'broker_name': broker_name
        }
