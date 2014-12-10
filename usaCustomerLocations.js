#!/usr/bin/env node

// Load the csvObject module
var csvObject = require('csvObject');
var objectListConstructor = require('objectList');
var objectList;







// ------------------------------------------------------------------//

// Create the filter object that has property names that
// match a column names.  The property names are methods
// that will recive the string for each row for that column
// and an object with the other columns for that row.  The 
// method returns the modified row for that column.
var importFiltersContructor = function () {
    "use strict";

    var that;

    // methods
    var getFilterObject,
        fillInCountry,
        partnerLevel,
        cleanAddress;


    // private properties
    var my = {};

    my.filters = {};


    fillInCountry = function (string, theObject) {
        var country = string;
        var inferredCountry = "";
        var countryCode = "";

        // 
        if (theObject.hasOwnProperty('inferredCountry')) {
            inferredCountry = theObject.inferredCountry;
        }
        if (country === "") {
            country = inferredCountry;
        }

        if (country === "") {

            if (theObject.hasOwnProperty('iSOCountryCode')) {
                countryCode = theObject.iSOCountryCode;
            }

            country = countryCode;
        }

        country = country.toLowerCase();
        switch (country) {
            case "us":
            case "usa":
            case "united states":
                country = "United States";
                break;
            case "can":
            case "canada":
            case "ca":
                country = "Canada";
                break;
            default:
        }

        return country;
    };


    partnerLevel = function (partnerLevelString) {
        // Change field to either be "partner" or "nonpartner"

        // remove 'none'
        if (partnerLevelString === "none") {
            partnerLevelString = "";
        }

        if (partnerLevelString === "") {
            partnerLevelString = "nonpartner";
        } else {
            partnerLevelString = "partner";
        }

        return partnerLevelString;
    };

    cleanAddress = function (string) {
        var regex = /\^r/g;
        // if (string.match(regex)) {
        //     console.log(string.replace(regex,", "));
        // }

        return string.replace(regex, ", ");
    };

    // Wrap up all the filters in to one object that can be passed
    // to the CSVObject
    getFilterObject = function () {
        my.filters = {
            country: fillInCountry,
            partnershipLevel: partnerLevel,
            address: cleanAddress
        };

        return my.filters;
    };

    // All privilaged methods that become publicly accessable
    that = {};
    that.getFilterObject = getFilterObject;
    return that;
};






// ------------------------------------------------------------------//
var outputFiltersContructor = function (args) {
    "use strict";

    var that, settings;

    // methods
    var getFilterObject,
        fillInCity,
        fillInState,
        fillInZipCode,
        fillInGroup,
        fillInDescription,
        fillFirstLastName;


    // Makes args optional
    if (args === undefined) {
        args = {};
    }
    // Set defaults
    settings = {
        fileGroup: args.fileGroup || ""
    };


    // private properties
    var my = {};

    my.filters = {};

    fillInCity = function (string, theObject) {
        // Maybe remove city if it is in canada?

        var city = "";
        if (theObject.hasOwnProperty('city')) {
            city = theObject.city;
        }

        var inferredCity = "";
        if (theObject.hasOwnProperty('inferredCity')) {
            inferredCity = theObject.inferredCity;
        }

        if (city === "") {
            city = inferredCity;
        }

        return city;
    };

    fillInState = function (string, theObject) {
        var state = "";
        if (theObject.hasOwnProperty('state')) {
            state = theObject.state;
        }

        var inferredStateRegion = "";
        if (theObject.hasOwnProperty('inferredStateRegion')) {
            inferredStateRegion = theObject.inferredStateRegion;
        }

        if (state === "") {
            state = inferredStateRegion;
        }

        return state;
    };


    fillInZipCode = function (string, theObject) {
        var postalCode = "";
        if (theObject.hasOwnProperty('postalCode')) {
            postalCode = theObject.postalCode;
        }

        var inferredPostalCode = "";
        if (theObject.hasOwnProperty('inferredPostalCode')) {
            inferredPostalCode = theObject.inferredPostalCode;
        }

        if (postalCode === "") {
            postalCode = inferredPostalCode;
        }

        return postalCode;
    };


    fillInGroup = function (string, theObject) {
        var customerLevel = theObject.customerLevel;
        var partnershipLevel = theObject.partnershipLevel;
        var fileGroup = settings.fileGroup;
        var group;

        if (customerLevel === "") {
            customerLevel = "C";
        }

        if (settings.fileGroup === "none") {
            group = fileGroup;
        } else {
            group = fileGroup + "/" + customerLevel + "/" + partnershipLevel;
        }

        return group;
    };

    fillInDescription = function (string, theObject) {
        var desc = "";
        var partnerLevel = theObject.partnershipLevel;

        if (partnerLevel === 'nonpartner') {
            partnerLevel = "";
        }

        desc = desc + "Address: " + theObject.address + "<br>";
        desc = desc + "CusCode: " + theObject.cusCode + "<br>";

        if (settings.fileGroup === "none") {
            desc = desc + "Last Purchase: " + settings.fileGroup + " store: " + theObject.store + "<br>";
        } else {
            desc = desc + "Last Purchase: " + settings.fileGroup + " store: " + theObject.store + "<br>";
            desc = desc + "Customer Level: " + theObject.customerLevel + " " + partnerLevel + "<br>";
        }

        return desc;
    };

    fillFirstLastName = function (string, theObject) {

        return theObject.firstName + " " + theObject.lastName;
    };


    // Wrap up all the filters in to one object that can be passed
    // to the CSVObject
    getFilterObject = function () {
        my.filters = {
            city: fillInCity,
            state: fillInState,
            postalCode: fillInZipCode,
            group: fillInGroup,
            description: fillInDescription,
            firstLastName: fillFirstLastName
        };

        return my.filters;
    };

    // All privilaged methods that become publicly accessable
    that = {};
    that.getFilterObject = getFilterObject;
    return that;
};

// ------------------------------------------------------------------//
var reportColumns = [
    'firstLastName',
    'emailAddress',
    'cusCode',
    'address',
    'city',
    'state',
    'country',
    'postalCode',
    'group',
    'description',
    'latitude',
    'longitude',
    'locationType',
    'formattedAddress'
];




// Utility Function called when all tasks are done.
var printAllDone = function () {
    'use strict';
    console.log('All Done!');
};



var customerDataFiles = {
    "2008": "example1.csv",
    "none": "example2.csv"
};





var customerDataFilesPath = "/Volumes/Customer Data/Customer Mapping/Raw Data/";
var outputPath = "/Volumes/Customer Data/Customer Mapping/";

var propertName, fileGroup, fileName, importReport;

var async = require("async");
var tasks = [];
var importedFiles = [];
var concatObjectLists;
var writeObjectLists;

for (propertName in customerDataFiles) {

    fileGroup = propertName;
    fileName  = customerDataFiles[propertName];

    // The imported Report read from disk
    importReport = csvObject({
            path: customerDataFilesPath + fileName,
            transformationObject: importFiltersContructor().getFilterObject(),
            reportName: fileGroup
        });


    // Read the input report
    // get the csvObjects and pass them to the outputReport
    // limit the output report to the reportColumns
    // Write it all back out.
    // importReport.readAllObjects();
    importReport.setColumnNames(reportColumns);
    tasks.push(importReport.readAllObjects);
    importedFiles.push(importReport);


}


writeObjectLists = function (error, objectList) {
    'use strict';

    var concatinatedOutputReport = csvObject({
        path: outputPath + "AllCustomers.csv"
    });
    concatinatedOutputReport.setColumnNames(reportColumns);
    concatinatedOutputReport.setCSVObjects(objectList);
    concatinatedOutputReport.writeAllObjects(printAllDone);
};



concatObjectLists = function () {
    'use strict';
    var csvObjects = [];
    var outputCsvObjects = [];
    var importReportObject;
    var outputReport;
    var reportName;
    var outputFilter;

    for (var i in importedFiles) {
        importReportObject = importedFiles[i];
        csvObjects = importReportObject.getCSVObjects();
        reportName = importReportObject.getReportName();
        outputFilter = outputFiltersContructor({"fileGroup": reportName}).getFilterObject();
        outputReport = csvObject({
                path: customerDataFilesPath + reportName + ".csv",
                transformationObject: outputFilter
            });
        outputReport.setColumnNames(reportColumns);
        outputReport.setCSVObjects(csvObjects);

        outputCsvObjects = outputCsvObjects.concat(outputReport.getCSVObjects());
    }

    // The output Report which will take the import Report
    // transform each row of the filtered columns
    // and write out only the 'reportColumns'


    console.log('Number of customers in report: ' + outputCsvObjects.length);
    objectList = objectListConstructor({'objectList': outputCsvObjects});
    objectList.geocode(writeObjectLists);
};




async.parallel(tasks, concatObjectLists);


