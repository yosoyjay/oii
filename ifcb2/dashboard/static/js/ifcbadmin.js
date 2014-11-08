

// create AngularJS application
var ifcbAdmin = angular.module('ifcbAdmin', ['ngRoute','restangular']);

// configure Restangular for flask restless endpoints
ifcbAdmin.config(function(RestangularProvider) {

    RestangularProvider.setBaseUrl('/admin/api/v1');

    RestangularProvider.setResponseExtractor(function(response, operation) {
        // This is a get for a list
        var newResponse;
        if (operation === 'getList') {
            // Return the result objects as an array and attach the metadata
            newResponse = response.objects;
            newResponse.metadata = {
                numResults: response.num_results,
                page: response.page,
                totalPages: response.total_pages
            };
        } else {
            // This is an element
            newResponse = response;
        }
      return newResponse;
    });
});

// confirmation popup
ifcbAdmin.directive('ngConfirmClick', [function() {
    return {
        restrict: 'A',
        link: function(scope, element, attrs) {
            element.bind('click', function() {
                var condition = scope.$eval(attrs.ngConfirmCondition);
                if(condition) {
                    var message = attrs.ngConfirmMessage;
                    if (message && confirm(message)) {
                        scope.$apply(attrs.ngConfirmClick);
                    }
                } else {
                    scope.$apply(attrs.ngConfirmClick);
                }
            });
        }
    }
}]);


// nav controller
ifcbAdmin.controller('NavigationCtrl', ['$scope', '$location', function ($scope, $location) {
    $scope.isCurrentPath = function (path) {
      return $location.path() == path;
    };
}]);


ifcbAdmin.controller('TimeSeriesCtrl', ['$scope', 'Restangular', function ($scope, Restangular) {

    // initialize local scope
    var baseTimeSeries = Restangular.all('time_series');
    $scope.alert = null;
    var restore = {};

    // load iniital data from api
    baseTimeSeries.getList().then(function(serverResponse) {
        $scope.time_series = serverResponse;
    }, function(errorResponse) {
        console.log(errorResponse);
        $scope.alert = 'Unexpected ' + errorResponse.status.toString()
            + ' error while loading data from server.'
    });

    // create new timeseries
    $scope.addNewTimeSeries = function() {
        $scope.time_series.push({label:'',description:'',data_dirs:[{path:'',product_type:'raw'}],edit:'true'});
        return true;
    }

    // create new
    $scope.addNewPath = function(ts) {
        ts.data_dirs.push({path:'',product_type:'raw'});
    }

    // mark timeseries group for editing
    $scope.editTimeSeries = function(ts) {
        restore[ts.id] = {};
        angular.copy(ts, restore[ts.id]);
        ts.edit = true;
    }

    // mark timeseries group for editing
    $scope.cancelTimeSeries = function(ts) {
        if (ts.id) {
            // cancel edit on saved timeseries
            // restore unedited copy
            angular.copy(restore[ts.id], ts);
            delete restore[ts.id];
        } else {
            // cancel creation of new timeseries
            $scope.time_series  = _.without($scope.time_series, ts);
        }
    }

    // save timeseries group to server
    $scope.saveTimeSeries = function(ts) {
	console.log("saving time series "+ts.label);
        // remove blank paths before save
        for (var i = 0; i < ts.data_dirs.length; i++) {
            if (ts.data_dirs[i].path.trim() == "") {
                $scope.removePath(ts, ts.data_dirs[i]);
            }
        }
	// perform accession on the time series (FIXME this may be slow!)
	// FIXME this uses jQuery
	accession_url = "/" + ts.label + "/api/accession";
	$scope.alert = "Saving time series...";
	$.getJSON(accession_url, function(r) {
	    var total = r.total
	    $scope.alert = total + " bin(s) found";
	});
        if(ts.id) {
	    // timeseries group already exists on server. update.
	    ts.patch().then(function(serverResponse) {
                delete ts.edit;
                delete restore[ts.id];
                $scope.alert = null;
	    }, function(serverResponse) {
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
	    });
        } else {
	    // new timeseries group. post to server.
	    baseTimeSeries.post(ts).then(function(serverResponse) {
                // copy server response to scope object
                angular.copy(serverResponse, ts);
                $scope.alert = null;
	    }, function(serverResponse) {
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
	    });
        }
    }

    // remove timeseries group
    $scope.removeTimeSeries = function(ts) {
        ts.remove().then(function() {
            $scope.time_series = _.without($scope.time_series, ts);
        });
    }

    // remove path
    $scope.removePath = function(ts,p) {
        // remove only from local scrope
        // server is updated with saveTimeSeries()
        ts.data_dirs = _.without(ts.data_dirs, p);
    }

}]);


ifcbAdmin.controller('InstrumentCtrl', ['$scope', 'Restangular', function ($scope, Restangular) {


    // initialize local scope
    var baseInstruments = Restangular.all('instruments');
    $scope.alert = null;

    // initialize local scope
    var baseTimeSeries = Restangular.all('time_series');
    $scope.alert = null;
    var restore = {};

    // load iniital data from api
    baseTimeSeries.getList().then(function(serverResponse) {
        $scope.time_series = serverResponse;
    }, function(errorResponse) {
        console.log(errorResponse);
        $scope.alert = 'Unexpected ' + errorResponse.status.toString()
            + ' error while loading data from server.'
    });

    // load iniital data from api
    baseInstruments.getList().then(function(serverResponse) {
        $scope.instruments = serverResponse;
    }, function(errorResponse) {
        console.log(errorResponse);
        $scope.alert = 'Unexpected ' + errorResponse.status.toString()
            + ' error while loading data from server.'
    });

    // create new timeseries
    $scope.addNewInstrument = function() {
        $scope.instruments.push({name:'',data_path:'',edit:true});
        return true;
    }


    // mark timeseries group for editing
    $scope.editInstrument = function(instr) {
        restore[instr.id] = {};
        angular.copy(instr, restore[instr.id]);
        instr.edit = true;
    }

    // mark timeseries group for editing
    $scope.cancelInstrument = function(instr) {
        if (instr.id) {
            // cancel edit on saved timeseries
            // restore unedited copy
            angular.copy(restore[instr.id], instr);
            delete restore[instr.id];
        } else {
            // cancel creation of new timeseries
            $scope.instruments  = _.without($scope.instruments, instr);
        }
    }

    // save timeseries group to server
    $scope.saveInstrument = function(instr) {
        if(instr.id) {
        // timeseries group already exists on server. update.
        instr.patch().then(function(serverResponse) {
                delete instr.edit;
                delete restore[instr.id];
                $scope.alert = null;
        }, function(serverResponse) {
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
        });
        } else {
        // new timeseries group. post to server.
        baseInstruments.post(instr).then(function(serverResponse) {
                // copy server response to scope object
                angular.copy(serverResponse, instr);
                $scope.alert = null;
        }, function(serverResponse) {
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
        });
        }
    }

    // remove timeseries group
    $scope.removeInstrument = function(instr) {
        instr.remove().then(function() {
            $scope.instruments = _.without($scope.instruments, instr);
        });
    }

}]);


// users controller
ifcbAdmin.controller('UserCtrl', ['$scope', 'Restangular', function ($scope, Restangular) {

    // initialize local scope
    var baseUsers = Restangular.all('users');
    $scope.alert = null;
    var restore = {};

    // load iniital data from api
    baseUsers.getList().then(function(serverResponse) {
        $scope.users = serverResponse;
    }, function(errorResponse) {
        console.log(errorResponse);
        $scope.alert = 'Unexpected ' + errorResponse.status.toString()
            + ' error while loading data from server.'
    });

    // save user to server
    $scope.saveUser = function(user) {
        // user will log in with email + password
        // so here username will always be set to email address
        user.username = user.email;
        if(user.id) {
            // user already exists on server. update.
            user.patch().then(function(serverResponse) {
                delete user.edit;
                delete restore[user.id];
                $scope.alert = null;
            }, function(serverResponse) {
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
            });
        } else {
            // new user. post to server.
            baseUsers.post(user).then(function(serverResponse) {
                // copy server response to scope object
                angular.copy(serverResponse, user);
                $scope.alert = null;
            }, function(serverResponse) {
		console.log("OK, that didn't work"); // FIXME debug
                console.log(serverResponse);
                $scope.alert = serverResponse.data.validation_errors;
            });
        }
    }

    // create new user
    $scope.addNewUser = function() {
	user = {first_name:'', last_name:'', email:'', is_enabled:true, edit:true};
        $scope.users.push(user);
        return true;
    }

    // cancel new user creation
    $scope.cancelUser = function(user) {
        if (user.id) {
            // cancel edit on saved timeseries
            // restore unedited copy
            angular.copy(restore[user.id], user);
            delete restore[user.id];
        } else {
            $scope.users = _.without($scope.users, user);
        }
    }

    $scope.editUser = function(user) {
        restore[user.id] = {}
        angular.copy(user, restore[user.id]);
        user.edit = true;
    }

    // disable user toggle
    $scope.toggleUser = function(user) {
        tmpuser = user.clone();
        tmpuser.is_enabled = !tmpuser.is_enabled;
        tmpuser.patch().then(function(serverResponse) {
            user.is_enabled = !user.is_enabled;
        });
    }

    $scope.setPassword = function(user) {
        $scope.userpw = user;
    }

    $scope.cancelPassword = function(user) {
        $scope.userpw = false;
    }

    $scope.pushPassword = function() {
        var pwchange = Restangular.one("setpassword", $scope.userpw.id);
        pwchange.customPOST({'password':$scope.userpw.password},'',{},{}).then(function(serverResponse) {
            console.log(serverResponse);
            $scope.userpw = false;
        });
    }

}]);


// my account controller
ifcbAdmin.controller('AccountCtrl', ['$scope', function ($scope) {

    $scope.myaccount = [];
}]);

// define application routes
ifcbAdmin.config(['$routeProvider', function($routeProvider) {
    $routeProvider.
        when('/time_series', {
            controller: 'TimeSeriesCtrl',
            templateUrl: 'views/TimeSeries.html'
            }).
        when('/users', {
            controller: 'UserCtrl',
            templateUrl: 'views/Users.html'
            }).
        when('/myaccount', {
            controller: 'AccountCtrl',
            templateUrl: 'views/MyAccount.html'
            }).
        when('/instruments', {
            controller: 'InstrumentCtrl',
            templateUrl: 'views/Instruments.html'
	    }).
        otherwise({
            redirectTo: '/time_series'
        });
}]);
