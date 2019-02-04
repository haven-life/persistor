// internal modules
import { Query as QueryNamespace} from './knex/Query';
import { Database as DatabaseNamespace} from './knex/Database';
import { Update as UpdateNamespace} from './knex/Update';
  
export namespace Knex {

    export import Database = DatabaseNamespace;

    export import Query = QueryNamespace;

    export import Update = UpdateNamespace;

}
