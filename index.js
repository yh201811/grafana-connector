"use strict";
const joi = require("joi");
const _ = require("lodash");
const Mustache = require("mustache");
const { aql, db, query } = require("@arangodb");
const { context } = require("@arangodb/locals");
const createRouter = require("@arangodb/foxx/router");
const { getAuth } = require("./util");

function* cartesian(args) {
  let remainder = args.length > 1 ? cartesian(args.slice(1)) : [[]];
  for (let r of remainder)
    for (let h of args[0])
      yield [h, ...r];
}

const htmlDecode = function (str) {
  const map = {
    '&amp;': '&',
    '&gt;': '>',
    '&lt;': '<',
    '&quot;': '"',
    '&#39;': "'"
  };
  const re = new RegExp('(' + Object.keys(map).join('|') + '|&#[0-9]{1,5};|&#x[0-9a-fA-F]{1,4};' + ')', 'g');
  return String(str).replace(re, function (match, capture) {
    return (capture in map) ? map[capture] :
      capture[2] === 'x' ?
        String.fromCharCode(parseInt(capture.substr(3), 16)) :
        String.fromCharCode(parseInt(capture.substr(2), 10));
  });
};

const AGGREGATIONS = [
  "AVERAGE",
  "COUNT",
  "COUNT_DISTINCT",
  "MAX",
  "MIN",
  "SORTED_UNIQUE",
  "STDDEV_POPULATION",
  "STDDEV_SAMPLE",
  "SUM",
  "UNIQUE",
  "VARIANCE_POPULATION",
  "VARIANCE_SAMPLE",
  "NONE"
];

const AGGREGATIONS_ALIASES = {
  "AVG": "AVERAGE",
  "COUNT_UNIQUE": "COUNT_DISTINCT",
  "LENGTH": "COUNT",
  "STDDEV": "STDDEV_POPULATION",
  "VARIANCE": "VARIANCE_POPULATION"
};

const ATTRIBUTE_NAME = RegExp("^[a-zA-Z][a-zA-Z0-9_]*$");

/** @type {{
 *   username: string,
 *   password: string,
 *   target: string,
 *   collection: string,
 *   aggregation: string,
 *   filterExpression: string,
 *   dateName: string,
 *   dateField: string,
 *   valueName: string,
 *   valueField: string
 * }} */
const cfg = context.configuration;
const TARGETS = {};

const parse_variable = function (d) {
  let values = _.map(_.split(d, ","), str => str.trim());

  if (values.length === 0) {
    values = [""];
  }

  return values;
};

const hideEmpty = cfg['hideEmpty']
const logQuery = cfg['logQuery']
//new add
var collectionCheck = true

let agg = cfg['aggregation'];
agg = agg ? agg.toUpperCase(agg) : null;

if (AGGREGATIONS_ALIASES[agg]) {
  agg = AGGREGATIONS_ALIASES[agg];
}

const aggregations = (agg && agg !== '*')
  ? parse_variable(agg)
  : AGGREGATIONS;

{
  const target = cfg['target'];
  const collection = cfg['collection'];

  const view = {};

  for (let a = 0; a < aggregations.length; ++a) {
    const aggregation = aggregations[a];
    view['aggregation'] = aggregation;

    const t = Mustache.render(target, view);

    let { filterExpression,
      dateName, dateField,
      valueName, valueField,
      alias } = cfg;

    const collectionName = Mustache.render(collection, view);
    
    // Make MultipleTemplateVariables can be used in collection section.
    var c = db._collection(collectionName);

    if (!c) {
      c = collection
      collectionCheck = false
    }

    TARGETS[t] = {
      target: t,
      alias,
      collection: c,
      view: _.clone(view),
      aggregation,
      filterExpression,
      dateField, dateName,
      valueField, valueName,
    };
  }
}

const TARGET_KEYS = _.keys(TARGETS);

const router = createRouter();
context.use(router);

router.use((req, res, next) => {
  const auth = getAuth(req);
  if (!auth || !auth.basic) {
    res.throw(401, "Authentication required");
  }
  const { username, password } = auth.basic;
  if (
    username !== cfg.username ||
    (cfg.password && password !== cfg.password)
  ) {
    res.throw(403, "Bad username or password");
  }
  next();
});

router
  .get("/", (_req, res) => {
    res.json({ ok: true });
  })
  .summary("SimpleJSON self-test endpoint")
  .description(
    "This is a dummy endpoint used by the SimpleJSON data source to " +
    "confirm that the data source is configured correctly."
  );

router
  .post("/search", (req, res) => {
    const body = req.body;

    if (body) {
      const j = JSON.parse(body);

      if (j.target) {
        const target = j.target;
        const tv = cfg['templateVariables'];

        if (tv[target]) {
          const values = db._query(tv[target]).toArray();
          res.json(values);
          return;
        }
      }
    }

    res.json(TARGET_KEYS);
  })
  .summary("List the available metrics")
  .description(
    "This endpoint is used to determine which metrics (collections) " +
    "are available to the data source."
  );

const seriesQuery = function (definition, vars, start, end, interval, isTable) {
  const agg = definition.aggregation && definition.aggregation !== "NONE"
    ? aql.literal(definition.aggregation)
    : null;
  // Using MultipleTemplateVariables for collection
  let { collection } = definition;
  if (!collectionCheck) {
    collection = aql.literal(collection
      ? `${collection}`
      : "");
  }

  let { filterExpression,
    dateName, dateField,
    valueName, valueField } = definition;

  filterExpression = filterExpression ? Mustache.render(filterExpression, vars) : undefined;

  dateField = dateField ? Mustache.render(dateField, vars) : undefined;
  definition.dateName = dateName ? Mustache.render(dateName, vars) : dateField;

  valueField = valueField ? Mustache.render(valueField, vars) : undefined;
  definition.valueName = valueName ? Mustache.render(valueName, vars) : valueField;

  let filterSnippet = aql.literal(filterExpression
    ? `FILTER ${filterExpression}`
    : "");

  let dateSnippet = aql.literal(ATTRIBUTE_NAME.test(dateField)
    ? `LET d = doc["${dateField}"]`
    : `LET d = ${dateField}`);

  let valueSnippet = aql.literal(ATTRIBUTE_NAME.test(valueField)
    ? `LET v = doc["${valueField}"]`
    : `LET v = ${valueField}`);

  let q;

  if (isTable) {
    q = aql`
      FOR doc IN ${collection}
        ${dateSnippet}
        FILTER d >= ${start} AND d < ${end}
        ${filterSnippet}
        ${valueSnippet}
        SORT d
        RETURN [d, v]
    `;
  } else if (agg) {
    q = aql`
      FOR doc IN ${collection}
        ${dateSnippet}
        FILTER d >= ${start} AND d < ${end}
        ${filterSnippet}
        ${valueSnippet}
        COLLECT date = FLOOR(d / ${interval}) * ${interval}
        AGGREGATE value = ${agg}(v)
        SORT date
        RETURN [value, date]
    `;
  } else {
    q = aql`
      FOR doc IN ${collection}
        ${dateSnippet}
        FILTER d >= ${start} AND d < ${end}
        ${filterSnippet}
        ${valueSnippet}
        SORT d
        RETURN [v, d]
    `;
  }

  if (logQuery) {
    console.log("query: " + q.query);
  }

  return db._query(q).toArray();
};

router
  .post("/query", (req, res) => {
    const body = req.body;
    const interval = body.intervalMs;
    const start = Number(new Date(body.range.from));
    const end = Number(new Date(body.range.to));
    const response = [];
    const unravel = function () { return [].slice.call(arguments); };

    const grafana = {};
    let multiKeys = [];
    let multiValues = [];

    // Logging targets in request 
    if (logQuery) {
      console.log("targets '" + JSON.stringify(body.targets) + "'");
    }

    if (cfg['multiValueTemplateVariables']) {
      let d = cfg['multiValueTemplateVariables'];
      multiKeys = _.map(_.split(d, ","), str => str.trim());
    }

    for (let key of multiKeys) {
      if (key in body.scopedVars) {
        let value = body.scopedVars[key].value;

        if (!Array.isArray(value)) {
          value = [value];
        }

        let l = [];

        for (let v of value) {
          let obj = {};
          obj[key] = htmlDecode(v);
          l.push(obj);
        }

        multiValues.push(l);
      }
      // Making MultipleTemplateVariables can be used by different sections by adding values to MultipleTemplateVariables through AQL
      else {
        const tv = cfg['templateVariables'];

        if (tv[key]) {
          const value = db._query(tv[key]).toArray();
          let l = [];

          for (let v of value) {
            let obj = {};
            obj[key] = htmlDecode(v);
            l.push(obj);
          }

          multiValues.push(l);
        }
      }
    }

    if (multiValues.length > 0) {
      multiValues = unravel(...cartesian(multiValues));
    } else {
      multiValues = [[{}]];
    }

    for (let key of Object.keys(body.scopedVars)) {
      if (key[0] !== '_' && !multiValues.includes(key)) {
        const val = body.scopedVars[key];
        grafana[key] = htmlDecode(val.value);

        if (logQuery) {
          console.log("using grafana var '" + key + "': '" + grafana[key] + "'");
        }
      }
    }

    // Logging values of MultipleTemplateVariables
    if (logQuery) {
      console.log("MultipleTemplateVariables '" + JSON.stringify(multiValues) + "'");
    }
    for (let mv of multiValues) {
      // Logging current used multiValues
      for (let { target, type, data } of body.targets) {
        let original = target;
        
        // Create target array if passed target is a multiple selected grafana variables.
        let originals = [];
        var regex1 = /^\((.*\|.*)\)$/g;
        var regex2 = /\\/g;
        if (original.match(regex1)) {
          originals = _.map(_.split(original.replace(regex2, "").replace("(", "").replace(")", ""), "|"), str => str.trim());
        }
        else {
          originals = [original.replace(regex2, "")]
        }

        let vars = _.assign({ grafana }, {"aggregation": "NONE"}, data);

        for (let m of mv) {
          if (logQuery) {
            console.log("using multi-value vars '" + JSON.stringify(m) + "'");
          }

          vars.grafana = _.assign(vars.grafana, m);
        }

        // Make MultipleTemplateVariables can be used in target section
        let continueFlag = false
        let targetDef = {}

        for (let orig of originals) {
          targetDef = TARGETS[orig];
          for (let t of Object.keys(TARGETS)) {
            let tVar = Mustache.render("{{" + t + "}}", vars);
            if (t.includes(orig) || (tVar == orig)) {
              targetDef = TARGETS[t];
              targetDef["target"] = orig;
  
              for (let m of mv) {
                if (orig == Object.values(m)) {
                  continueFlag = true;
                  if (logQuery) {
                    console.log("target '" + orig + "'");
                  }
                  break;
                }
              }
  
              break;
            }
          }

          if (continueFlag) {
            break;
          }
        }   

        // if (!targetDef) {
        //   throw Error(`unknown target ${original}`);
        // }

        if (!continueFlag) {
          if (logQuery) {
            console.log("current multi-value vars do not match current target.");
          }
          continue;
        }

        const definition = _.merge({}, targetDef);
        vars.aggregation = definition.view.aggregation;

        if (targetDef.alias) {
          target = Mustache.render(targetDef.alias, vars);
        }
        // create different target names in response with current MultipleTemplateVariables
        else {
          target = "";
          for (let m of mv) {
            target += "{" + Object.values(m) + "}";
          }
        }

        if (data && data.alias) {
          target = Mustache.render(data.alias, vars);
        }

        // Using MultipleTemplateVariables for collection
        if (!collectionCheck) {
          definition.collection = Mustache.render(definition.collection, vars);
          let c1 = db._collection(definition.collection);
          if (!c1) {
            throw new Error(
              `Invalid service configuration. Unknown collection: ${definition.collection}`
            );
          }
        }

        const isTable = (type === "table");
        const datapoints = definition ?
          seriesQuery(definition, vars, start, end, interval, isTable) :
          [];

        if (datapoints.length > 0 || !hideEmpty) {
          if (isTable) {
            response.push({
              target: target,
              type: "table",
              columns: [{ text: definition.dateName }, { text: definition.valueName }],
              rows: datapoints
            });
          } else {
            response.push({
              target: target,
              type: "timeserie",
              datapoints
            });
          }
        }
      }
    }

    res.json(response);
  })
  .body(
    joi
      .object({
        intervalMs: joi.number().required(),
        range: joi
          .object({
            from: joi.string().required(),
            to: joi.string().required(),
            raw: joi.any().optional()
          })
          .required(),
        targets: joi
          .array()
          .items(
            joi
              .object({
                target: joi.allow(...TARGET_KEYS).required(),
                type: joi.allow("timeserie", "table").required()
              })
              .required()
          )
          .required()
      })
      .options({ allowUnknown: true })
  )
  .summary("Perform a SimpleJSON query")
  .description(
    "This endpoint performs the actual query for one or more metrics in a given time range. Results are aggregated with the given interval."
  );
