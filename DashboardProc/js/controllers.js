app.controller("EGMGDI_bullet", function ($scope, $http) {
    $http.get("web/General/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#EGMGDI_bullet", title=true);
    });
});

app.controller("EGMGDI_donut", function ($scope, $http) {
    $http.get("web/General/donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#EGMGDI_donut");     
    });
});

app.controller("V_bullet", function ($scope, $http) {
    $http.get("web/Vicepr/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#V_bullet", title=false);
    });
});

app.controller("V_donut", function ($scope, $http) {
    $http.get("web/Vicepr/donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#V_donut");     
    });
});

app.controller("JyL_donut", function ($scope, $http) {
    $http.get("web/Jefes/donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#JyL_donut");     
    });
});

app.controller("JyL_bullet", function ($scope, $http) {
    $http.get("web/Jefes/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#JyL_bullet", title=false);
    });
});

app.controller("ACI_donut", function ($scope, $http) {
    $http.get("web/Internacionales/donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#ACI_donut");     
    });
});

app.controller("ACI_bullet", function ($scope, $http) {
    $http.get("web/Internacionales/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#ACI_bullet", title=false);
    });    
});

app.controller("ACN_donut", function ($scope, $http) {
    $http.get("web/Nacionales/donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#ACN_donut");     
    });
});

app.controller("ACN_bullet", function ($scope, $http) {
    $http.get("web/Nacionales/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#ACN_bullet", title=false);
    });
});


