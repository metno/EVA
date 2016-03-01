import eva.base.adapter


class NullAdapter(eva.base.adapter.BaseAdapter):
    """
    An adapter that matches nothing and does nothing.
    """

    def process_resource(self, *args, **kwargs):
        self.logger.info('NullAdapter has successfully sent the resource to /dev/null')
