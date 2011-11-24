#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Gregory Sitnin <sitnin@gmail.com>"
__copyright__ = "WWPass Corporation, 2011"


def all():
    return {
        "month_ru": month_ru,
        "hum_month_ru": hum_month_ru,
    }

def month_ru(month):
    months = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    return months[int(month)-1]

def hum_month_ru(dt, year=False):
    months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    result = "%d %s"%(dt.day, months[dt.month-1])
    if year:
        result += " %d года"%dt.year
    return result
