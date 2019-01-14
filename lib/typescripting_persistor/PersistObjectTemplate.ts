import * as _ from 'underscore';

import {ObjectTemplate} from 'supertype';

// @TODO: Need to export ObjectTemplate as well

// This is kinda jank because it's not REALLY extending OT. This should really be a mixin.
// But typescript mixins are bad. 
// Actually I'm not sure if it should be a mixin even.

export class PersistObjectTemplate extends ObjectTemplate {

    static DB_Knex = 'knex';
    static DB_Mongo = 'mongo';
    static schemaVerified: boolean;

    static baseClassForPersist: typeof ObjectTemplate;

    static initialize(baseClassForPersist: typeof ObjectTemplate) {
        this.init();
        this.baseClassForPersist = baseClassForPersist;
    }

    
    /**
     * 
     * 
     *  API . TS
     * 
     * 
     */

    /**
    * PUBLIC INTERFACE FOR OBJECTS
    */
    static getPersistorProps() {
        let persistorProps = {};

        Object.keys(this.__dictionary__).forEach((key, index) => {
            let template = this.__dictionary__[key];

            let props = template.getProperties(false); // @TODO: Need to change typing to make includeVirtual optional

            Object.keys(props).forEach(prop => {
                if (prop.match(/Persistor$/) && prop.substr(0, 2) != '__') {
                    persistorProps[template.__name__] = persistorProps[template.__name__] || {}
                    persistorProps[template.__name__][prop.replace(/Persistor$/, '')] = 1;
                }
            });
        });    

        return persistorProps;
    }
    
    /**
     * PUBLIC INTERFACE FOR TEMPLATES
     *
     * @param {supertype} template - load all parent/child/subdocument/subsetof defitions
     */
    static _injectIntoTemplate (template) {
        this._prepareSchema(template);
        this._injectTemplateFunctions(template);
        this._injectObjectFunctions(template);
    }

    static _prepareSchema(template) {
        if (!this.schemaVerified) {
            this._verifySchema();
        }
        this.schemaVerified = true;

        // Process subclasses that didn't have schema entries
        var parent = template.__parent__;
        while (!template.__schema__ && parent) {
            if (parent.__schema__) {
                template.__schema__ = parent.__schema__;
                template.__collection__ = parent.__collection__;
                template.__table__ = template.__schema__.table ? template.__schema__.table : parent.__table__;
                template.__topTemplate = parent.__topTemplate__;
                parent = null;
            } else {
                parent = parent.__parent__;
            }
        }

        // Process subsets
        if (template.__schema__ && template.__schema__.subsetOf) {
            var mainTemplate = this.__dictionary__[template.__schema__.subsetOf];
            if (!mainTemplate) {
                throw new Error('Reference to subsetOf ' + template.__schema__.subsetOf + ' not found for ' + template.__name__);
            }
            template.__subsetOf__ = template.__schema__.subsetOf
            if (!mainTemplate.__schema__) {
                parent = mainTemplate.__parent__;
                while (!mainTemplate.__schema__ && parent) {
                    if (parent.__schema__) {
                        mainTemplate.__schema__ = parent.__schema__;
                        mainTemplate.__collection__ = parent.__collection__;
                        mainTemplate.__table__ = mainTemplate.__schema__.table ? mainTemplate.__schema__.table : parent.__table__;
                        mainTemplate.__topTemplate = parent.__topTemplate__;
                        parent = null;
                    } else {
                        parent = parent.__parent__;
                    }
                }
                if (!mainTemplate.__schema__) {
                    throw new Error('Missing schema entry for ' + template.__schema__.subsetOf);
                }
            }
            mergeRelationships(template.__schema__, mainTemplate.__schema__);
            template.__collection__ = mainTemplate.__collection__;
            template.__table__ = mainTemplate.__table__;
        }
        this.baseClassForPersist._injectIntoTemplate(template);

        function mergeRelationships(orig, overlay) {
            _.each(overlay.children, function (value, key) {
                orig.children = orig.children || {};
                if (!orig.children[key]) {
                    orig.children[key] = value;
                }
            });
            _.each(overlay.parents, function (value, key) {
                orig.parents = orig.parents || {};
                if (!orig.parents[key]) {
                    orig.parents[key] = value;
                }
            });
        }
    }

}