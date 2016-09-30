import httmock


@httmock.urlmatch(path=r'^/api/v1/$')
def req_schema_base(url, request):
    return b"""
{
    "data": {
        "list_endpoint": "/api/v1/data/",
        "schema": "/api/v1/data/schema/"
    },
    "dataformat": {
        "list_endpoint": "/api/v1/dataformat/",
        "schema": "/api/v1/dataformat/schema/"
    },
    "datainstance": {
        "list_endpoint": "/api/v1/datainstance/",
        "schema": "/api/v1/datainstance/schema/"
    },
    "institution": {
        "list_endpoint": "/api/v1/institution/",
        "schema": "/api/v1/institution/schema/"
    },
    "kafka": {
        "list_endpoint": "/api/v1/kafka/",
        "schema": "/api/v1/kafka/schema/"
    },
    "license": {
        "list_endpoint": "/api/v1/license/",
        "schema": "/api/v1/license/schema/"
    },
    "person": {
        "list_endpoint": "/api/v1/person/",
        "schema": "/api/v1/person/schema/"
    },
    "product": {
        "list_endpoint": "/api/v1/product/",
        "schema": "/api/v1/product/schema/"
    },
    "productinstance": {
        "list_endpoint": "/api/v1/productinstance/",
        "schema": "/api/v1/productinstance/schema/"
    },
    "projection": {
        "list_endpoint": "/api/v1/projection/",
        "schema": "/api/v1/projection/schema/"
    },
    "servicebackend": {
        "list_endpoint": "/api/v1/servicebackend/",
        "schema": "/api/v1/servicebackend/schema/"
    },
    "variable": {
        "list_endpoint": "/api/v1/variable/",
        "schema": "/api/v1/variable/schema/"
    }
}
"""


@httmock.urlmatch(path=r'^/api/v1/productinstance/schema/$')
def req_schema_productinstance(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "complete": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A dictionary of data. Ex: {'price': 26.73, 'name': 'Daniel'}",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "dict",
            "unique": false,
            "verbose_name": "complete"
        },
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "id": {
            "blank": false,
            "default": "8c0ee8fb-7844-4e49-891a-5d39e7b7a509",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "product": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/product/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "product"
        },
        "reference_time": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "reference time"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "version": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "version"
        }
    },
    "filtering": {
        "id": 1,
        "product": 2,
        "reference_time": 1,
        "version": 1
    },
    "ordering": [
        "reference_time",
        "version"
    ]
}
"""


@httmock.urlmatch(path=r'^/api/v1/data/schema/$')
def req_schema_data(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "id": {
            "blank": false,
            "default": "5b84d470-c55a-4f42-b1c1-b480cd261ac4",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "productinstance": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/productinstance/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "productinstance"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "time_period_begin": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "time period begin"
        },
        "time_period_end": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "time period end"
        },
        "variables": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Many related resources. Can be either a list of URIs or list of individually nested resource data.",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/variable/schema/",
            "related_type": "to_many",
            "type": "related",
            "unique": false,
            "verbose_name": "variables"
        }
    },
    "filtering": {
        "id": 1,
        "productinstance": 2,
        "time_period_begin": 2,
        "time_period_end": 2
    }
}
"""


@httmock.urlmatch(path=r'^/api/v1/dataformat/schema/$')
def req_schema_dataformat(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "id": {
            "blank": false,
            "default": "2421ceed-bf73-40a4-b082-2eddc757e4d0",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "name": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "name"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "slug": {
            "blank": false,
            "default": "slugify",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "slug"
        }
    },
    "filtering": {
        "id": 1,
        "name": 1,
        "slug": 1
    }
}
"""


@httmock.urlmatch(path=r'^/api/v1/product/schema/$')
def req_schema_product(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "bounding_box": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "bounding box"
        },
        "contact": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/person/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "contact"
        },
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "file_count": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Integer data. Ex: 2673",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "file count"
        },
        "grid_resolution": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Fixed precision numeric data. Ex: 26.73",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "decimal",
            "unique": false,
            "verbose_name": "grid resolution"
        },
        "grid_resolution_unit": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "grid resolution unit"
        },
        "id": {
            "blank": false,
            "default": "b2213892-31ed-4af6-ace8-d5547e333a1f",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "institution": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/institution/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "institution"
        },
        "license": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/license/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "license"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "name": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "name"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "operational": {
            "blank": true,
            "default": "",
            "help_text": "Boolean data. Ex: True",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "boolean",
            "unique": false,
            "verbose_name": "operational"
        },
        "parents": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Many related resources. Can be either a list of URIs or list of individually nested resource data.",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/product/schema/",
            "related_type": "to_many",
            "type": "related",
            "unique": false,
            "verbose_name": "parents"
        },
        "prognosis_length": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Integer data. Ex: 2673",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "prognosis length"
        },
        "projection": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/projection/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "projection"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "slug": {
            "blank": false,
            "default": "slugify",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "slug"
        },
        "source": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/institution/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "source"
        },
        "source_key": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "source key"
        },
        "time_steps": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Integer data. Ex: 2673",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "time steps"
        },
        "wdb_data_provider": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "wdb data provider"
        }
    },
    "filtering": {
        "id": 1,
        "name": [
            "exact"
        ],
        "parents": 1,
        "slug": [
            "exact"
        ],
        "source": 2,
        "source_key": [
            "exact"
        ],
        "wdb_data_provider": [
            "exact"
        ]
    }
}
"""


@httmock.urlmatch(path=r'^/api/v1/servicebackend/schema/$')
def req_schema_servicebackend(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "documentation_url": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "documentation url"
        },
        "id": {
            "blank": false,
            "default": "15340982-c7e1-4710-beb6-b5225dc02932",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "name": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "name"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "slug": {
            "blank": false,
            "default": "slugify",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "slug"
        }
    },
    "filtering": {
        "id": 1,
        "name": 1,
        "slug": 1
    }
}
"""


@httmock.urlmatch(path=r'^/api/v1/datainstance/schema/$')
def req_schema_datainstance(url, request):
    return b"""
{
    "allowed_detail_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "allowed_list_http_methods": [
        "get",
        "post",
        "put",
        "delete",
        "patch"
    ],
    "default_format": "application/json",
    "default_limit": 20,
    "fields": {
        "created": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "created"
        },
        "data": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/data/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "data"
        },
        "deleted": {
            "blank": true,
            "default": false,
            "help_text": "Boolean data. Ex: True",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "boolean",
            "unique": false,
            "verbose_name": "deleted"
        },
        "expires": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "expires"
        },
        "format": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/dataformat/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "format"
        },
        "hash": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "hash"
        },
        "hash_type": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": true,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "hash type"
        },
        "id": {
            "blank": false,
            "default": "188d641b-4956-4fe1-a6a1-50407d7cf56c",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": true,
            "readonly": false,
            "type": "string",
            "unique": true,
            "verbose_name": "id"
        },
        "modified": {
            "blank": true,
            "default": true,
            "help_text": "A date & time as a string. Ex: \\"2010-11-10T03:07:43\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "datetime",
            "unique": false,
            "verbose_name": "modified"
        },
        "object_version": {
            "blank": false,
            "default": 0,
            "help_text": "Integer data. Ex: 2673",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "integer",
            "unique": false,
            "verbose_name": "object version"
        },
        "partial": {
            "blank": true,
            "default": false,
            "help_text": "Boolean data. Ex: True",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "boolean",
            "unique": false,
            "verbose_name": "partial"
        },
        "resource_uri": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": true,
            "type": "string",
            "unique": false,
            "verbose_name": "resource uri"
        },
        "servicebackend": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "A single related resource. Can be either a URI or set of nested resource data.",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "related_schema": "/api/v1/servicebackend/schema/",
            "related_type": "to_one",
            "type": "related",
            "unique": false,
            "verbose_name": "servicebackend"
        },
        "url": {
            "blank": false,
            "default": "No default provided.",
            "help_text": "Unicode string data. Ex: \\"Hello World\\"",
            "nullable": false,
            "primary_key": false,
            "readonly": false,
            "type": "string",
            "unique": false,
            "verbose_name": "url"
        }
    },
    "filtering": {
        "data": 2,
        "deleted": 1,
        "expires": 1,
        "format": 2,
        "id": 1,
        "partial": 1,
        "servicebackend": 2,
        "url": 1
    },
    "ordering": [
        "created",
        "modified",
        "expires",
        "partial"
    ]
}
"""


@httmock.urlmatch(path='^/api/v1/[^/]+/$', query=r'slug=.+')
def req_schema_slug(url, request):
    """!
    @brief Return a generic empty response.
    """
    return b"""
{
    "meta": {
        "limit": 20,
        "next": null,
        "offset": 0,
        "previous": null,
        "total_count": 1
    },
    "objects": [
        {
            "id": "foo"
        }
    ]
}
"""


SCHEMAS = (
    req_schema_base,
    req_schema_data,
    req_schema_dataformat,
    req_schema_datainstance,
    req_schema_product,
    req_schema_productinstance,
    req_schema_servicebackend,
    req_schema_slug,
)
