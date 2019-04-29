import * as schemaValidator from 'tv4';
import { PersistObjectTemplate } from './PersistObjectTemplate';
import { Persistent } from './Persistent';


// _validateParams from util.ts
export namespace SchemaValidator {

    let validateFetchSpecsRef: {[key: string]: any}; 

    /**
     * Helper function for new api test validations
     *
     * @export
     * @param {string} template
     * @returns
     */
    export function getValidateFetchSpecsRefByTemplate(template: string) {
        return validateFetchSpecsRef[template];
    }

    const persistSchema = {
            'type': 'object',
            'additionalProperties': false,
            'properties': {
                'transaction': {
                    type: ['null', 'object']
                },
                'cascade': {
                    type: 'boolean'
                },
                'logger': {
                    type: ['null', 'object']
                }
            }
    };

    const fetchSchema = {
            'type': 'object',
            'additionalProperties': false,
            'properties': {
                'fetch': {
                    type: ['null', 'object']
                },
                'projection': {
                    type: ['null', 'object']
                },
                'start': {
                    type: 'number'
                },
                'limit': {
                    type: 'number'
                },
                'order': {
                    type: ['null', 'object']
                },
                'transient': {
                    type: 'boolean'
                },
                'session': {
                    type: ['null', 'object']
                },
                'logger': {
                    type: ['null', 'object']
                },
                'enableChangeTracking': {
                    type: ['boolean', 'null', 'undefined']
                }
            }
    };

    const commitSchema = {
            'type': 'object',
            'additionalProperties': false,
            'properties': {
                'transaction': {
                    type: ['null', 'object']
                },
                'logger': {
                    type: ['null', 'object']
                },
                'notifyChanges': {
                    type: ['boolean', 'null', 'undefined']
                }
            }
        };

    const schemas = { commitSchema, persistSchema, fetchSchema };

    export function isFetchKeyInDefineProperties(key, template: typeof Persistent) {
        const templateProperties = template.getProperties();

        if (templateProperties[key]) {
            return getKeyTemplate(templateProperties[key]);
        }
        else {
            return template.__children__.reduce((keyTemplate, child) => {
                return keyTemplate || isFetchKeyInDefineProperties(key, child as typeof Persistent)
            }, null);
        }
    }

    export function getKeyTemplate(template) {
        if (template.type && template.type.isObjectTemplate) {
            return template.type;
        }
        else if (template.of && template.type === Array && template.of.isObjectTemplate) {
            return template.of;
        }
    }


    export function fetchPropChecks(fetch, template, name) {
        Object.keys(fetch).map((key) => {
            var keyTemplate = isFetchKeyInDefineProperties(key, template);
            if (keyTemplate) {
                if (!fetch[key].fetch) return;
                fetchPropChecks(fetch[key].fetch, keyTemplate, keyTemplate.__name__)
            }
            else {
                throw new Error(`Key used ${key} is not a valid fetch key for the template ${name}`);
            }
        });
    }

    // PersistObjectTemplate._validFetchSpecs
    function validateFetchSpec(template, options) {
        let validSpecs = validateFetchSpecsRef || {};
        //if the fetch spec currently used for the same template is already used, no need to valid again..

        const templateName = template.__name__;

        const replacedString = JSON.stringify(options.fetch).replace(/\"|\{|\}/g, '');

        if (!validSpecs[templateName] || !validSpecs[templateName][replacedString]) {
            fetchPropChecks(options.fetch, template, templateName);
            validSpecs[templateName] = validSpecs[templateName] || {};
            validSpecs[templateName][replacedString] = {};
            validateFetchSpecsRef = validSpecs;
        }

    }

    // _validateParams
    export function validate (options, schema, template?) {
        if (options) {

            if (!schemaValidator.validate(options, schemas[schema])) {

                const errDescription = schemaValidator.error.dataPath !== `` ? `Field: ${schemaValidator.error.dataPath}, ` : ``;
                const errMessage = `Parameter validation failed, ${errDescription}`;

                throw new Error(errMessage);
            }

            if (template && schema === 'fetchSchema' && !!options.fetch) {
                validateFetchSpec(template, options);
            }
        }
    }
}