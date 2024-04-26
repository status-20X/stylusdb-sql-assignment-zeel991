const {readCSV} = require('../../src/csvReader');
const {parseSelectQuery} = require('../../src/queryParser');
const {executeSELECTQuery} = require('../../src/index');

test('Read CSV File', async () => {
    const data = await readCSV('./student.csv');
    expect(data.length).toBeGreaterThan(0);
    expect(data.length).toBe(4);
    expect(data[0].name).toBe('John');
    expect(data[0].age).toBe('30'); //ignore the string type here, we will fix this later
});

test('Parse SQL Query', () => {
    const query = 'SELECT id, name FROM student';
    const parsed = parseSelectQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        table: 'student',
        whereClauses: [],
        joinCondition: null,
        joinTable: null,
        joinType: null,
        groupByFields: null,
        hasAggregateWithoutGroupBy: false,
        orderByFields: null,
        limit:null,
        isDistinct:false
    });
});

test('Execute SQL Query', async () => {
    const query = 'SELECT id, name FROM student';
    const result = await executeSELECTQuery(query);
    expect(result.length).toBeGreaterThan(0);
    expect(result[0]).toHaveProperty('id');
    expect(result[0]).toHaveProperty('name');
    expect(result[0]).not.toHaveProperty('age');
    expect(result[0]).toEqual({ id: '1', name: 'John' });
});

test('Parse SQL Query with WHERE Clause', () => {
    const query = 'SELECT id, name FROM student WHERE age = 25';
    const parsed = parseSelectQuery(query);
    expect(parsed).toEqual({
        fields: ['id', 'name'],
        table: 'student',
        whereClauses: [{
            "field": "age",
            "operator": "=",
            "value": "25",
        }],
        joinCondition: null,
        joinTable: null,
        joinType: null,
        groupByFields: null,
        hasAggregateWithoutGroupBy: false,
        orderByFields: null,
        limit:null,
        isDistinct:false
    });
});


test('Execute SQL Query with WHERE Clause', async () => {
    const query = 'SELECT id, name FROM student where age = 25';
    const result = await executeSELECTQuery(query);
    let newresult = result.map((obj) => {
        const newobj = {}
        for (const key in obj) {
            newobj[key.toLowerCase()]=obj[key]
        }
        return newobj
    })
    console.log("redsult",newresult)
    expect(newresult.length).toBe(1);
    expect(newresult[0]).toHaveProperty('id');
    expect(newresult[0]).toHaveProperty('name');
    expect(newresult[0].id).toBe('2');
});