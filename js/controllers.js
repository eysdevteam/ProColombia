app.controller("EGMGDI_bullet", function ($scope, $http) {
    $http.get("web/EGMGDI_bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#EGMGDI_bullet", title=true);
    });
});

app.controller("EGMGDI_donut", function ($scope, $http) {
    $http.get("web/EGMGDI_donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#EGMGDI_donut");     
    });
});

app.controller("V_bullet", function ($scope, $http) {
    $http.get("web/V_bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#V_bullet", title=false);
    });
});

app.controller("V_donut", function ($scope, $http) {
    $http.get("web/V_donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#V_donut");     
    });
});

app.controller("JyL_donut", function ($scope, $http) {
    $http.get("web/JyL_donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#JyL_donut");     
    });
});

app.controller("JyL_bullet", function ($scope, $http) {
    $http.get("web/JyL_bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#JyL_bullet", title=false);
    });
});

app.controller("ACI_donut", function ($scope, $http) {
    $http.get("web/ACI_donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#ACI_donut");     
    });
});

app.controller("ACI_bullet", function ($scope, $http) {
    $http.get("web/ACI_bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#ACI_bullet", title=false);
    });    
});

app.controller("ACN_donut", function ($scope, $http) {
    $http.get("web/ACN_donut/donut.json").then(function(data){
        $scope.dataset=data.data
        donut($scope.dataset, "#ACN_donut");     
    });
});

app.controller("ACN_bullet", function ($scope, $http) {
    $http.get("web/ACN_bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#ACN_bullet", title=false);
    });
});


