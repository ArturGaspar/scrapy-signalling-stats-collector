from scrapy import signals
from scrapy.statscol import MemoryStatsCollector, StatsCollector


class SignallingStatsDict(dict):

    stat_deleted = object()
    stat_set = object()

    def __init__(self, spider, crawler, values={}):
        super(SignallingStatsDict, self).__init__()
        self.spider = spider
        self.crawler = crawler
        self.update(values)

    def __delitem__(self, key):
        value = self.pop(key)
        self.crawler.signals.send_catch_log_deferred(signal=self.stat_deleted,
                                                     spider=self.spider,
                                                     key=key, value=value)

    def __setitem__(self, key, value):
        self.crawler.signals.send_catch_log_deferred(signal=self.stat_set,
                                                     spider=self.spider,
                                                     key=key, value=value)
        super(SignallingStatsDict, self).__setitem__(key, value)


class SignallingStatsCollector(StatsCollector):
    """Stats collector that emits signals for stats."""

    spider_closed = object()
    stat_set = object()
    stat_increased = object()
    stat_maxed = object()
    stat_minned = object()
    stats_cleared = object()

    def __init__(self, crawler):
        super(SignallingStatsCollector, self).__init__(crawler)
        self.crawler = crawler

    def _signal(self, signal, **kwargs):
        self.crawler.signals.send_catch_log_deferred(signal=signal,
                                                     stats=self.get_stats(),
                                                     **kwargs)

    def set_value(self, key, value, spider=None):
        self._signal(self.stat_set, spider=spider, key=key, value=value)
        super(SignallingStatsCollector, self).set_value(key, value, spider)

    def inc_value(self, key, count=1, start=0, spider=None):
        self._signal(self.stat_increased, spider=spider, key=key, count=count,
                     start=start)
        super(SignallingStatsCollector, self).inc_value(key, count, start,
                                                        spider)

    def max_value(self, key, value, spider=None):
        self._signal(self.stat_maxed, spider=spider, key=key, value=value)
        super(SignallingStatsCollector, self).max_value(key, value, spider)

    def min_value(self, key, value, spider=None):
        self._signal(self.stat_minned, spider=spider, key=key, value=value)
        super(SignallingStatsCollector, self).min_value(key, value, spider)

    def clear_stats(self, spider=None):
        self._signal(self.stats_cleared, spider=spider)
        super(SignallingStatsCollector, self).clear_stats(spider)

    def close_spider(self, spider, reason):
        self._signal(self.spider_closed, spider=spider, reason=reason)
        super(SignallingStatsCollector, self).close_spider(spider, reason)


class SignallingMemoryStatsCollector(MemoryStatsCollector,
                                     SignallingStatsCollector):
    pass


class StatsReporter(object):
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()

        crawler.signals.connect(ext.spider_opened,
                                signal=signals.spider_opened)
        crawler.signals.connect(ext.stat_set,
                                signal=SignallingStatsCollector.stat_set)
        crawler.signals.connect(ext.stat_increased,
                                signal=SignallingStatsCollector.stat_increased)
        crawler.signals.connect(ext.stat_maxed,
                                signal=SignallingStatsCollector.stat_maxed)
        crawler.signals.connect(ext.stat_minned,
                                signal=SignallingStatsCollector.stat_minned)
        crawler.signals.connect(ext.stats_cleared,
                                signal=SignallingStatsCollector.stats_cleared)
        crawler.signals.connect(ext.spider_closed,
                                signal=SignallingStatsCollector.spider_closed)

        return ext

    def spider_opened(self, spider):
        pass

    def stat_set(self, spider, key, value):
        pass

    def stat_increased(self, spider, key, count, start):
        pass

    def stat_maxed(self, spider, key, value):
        pass

    def stat_minned(self, spider, key, value):
        pass

    def stats_cleared(self, spider):
        pass

    def spider_closed(self, spider, reason, stats):
        pass

