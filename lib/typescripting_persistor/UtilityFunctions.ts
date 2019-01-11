import * as _ from 'underscore';

export namespace UtilityFunctions {
    export function processTemplate(template, persistorProps) {

        let props = template.getProperties();

        _.each(props, (_defineProperty, prop: string) => {
            if (prop.match(/Persistor$/) && prop.substr(0, 2) != '__') {
                persistorProps[template.__name__] = persistorProps[template.__name__] || {}
                persistorProps[template.__name__][prop.replace(/Persistor$/, '')] = 1;
            }
        });
    }

    function undefHandler(key, value) {
        return typeof(value)  === 'undefined' ? `undefined value provided for ${key}` : value;   
    }

    export function logExceptionAndRethrow(exception, logger, template, query, activity) {
        if (typeof(query) === 'undefined') {
            query = 'Undefined value provided for query';
        } else if (typeof(query) === 'object') {
            query = JSON.stringify(query, undefHandler);
        }

        logger.error(
            {
                component: 'persistor',
                module: 'api',
                activity: activity,
                data: 
                {
                    template: template,
                    query: query
                }
            });

        throw exception;
    }
}