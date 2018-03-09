app.controller("Q_S1", function($scope){
    $scope.datatest = [
        {"seccion":"Sección 1","contC":[5],"valor":[1,1,1,1,5]}      
    ];
    $scope.datamodal = [
            {"seccion":"Sección 1","valor":[1],"preguntas": ["pregunta3"]},
            {"seccion":"Sección 1","valor":[1],"preguntas": ["pregunta2"]},
            {"seccion":"Sección 1","valor":[1],"preguntas": ["pregunta1"]},
            {"seccion":"Sección 1","valor":[1],"preguntas": ["pregunta1"]},
            {"seccion":"Sección 1","valor":[5],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4","pregunta5"]}
        
    ];
    
 
    $scope.datatest[0]["preguntas"]=[];
    for(i = 0; i <= $scope.datamodal.length-1; i++){
        $scope.datatest[0].preguntas.push($scope.datamodal[i]);
    }    
    principalBullet($scope.datatest, "#Q_S1", title=false, ind=true);

});
app.controller("Q_S2", function($scope){
    $scope.datatest = [        
        {"seccion":"Sección 1","contC":[5],"valor":[5,4,6,7,1]}      
    ];
    $scope.datamodal = [
            {"seccion":"Sección 2","valor":[5],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4","pregunta5"]},
            {"seccion":"Sección 2","valor":[4],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4"]},
            {"seccion":"Sección 2","valor":[6],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4","pregunta5","pregunta6"]},
            {"seccion":"Sección 2","valor":[7],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4","pregunta5","pregunta6","pregunta7"]},
            {"seccion":"Sección 2","valor":[1],"preguntas": ["pregunta1"]}
       
    ];

    $scope.datatest[0]["preguntas"]=[];
    for(i = 0; i <= $scope.datamodal.length-1; i++){
        $scope.datatest[0].preguntas.push($scope.datamodal[i]);
    } 

    principalBullet($scope.datatest, "#Q_S2", title=false, ind=true);

});
app.controller("Q_S3", function($scope){
    $scope.datatest = [
        {"seccion":"Sección 3","contC":[5],"valor":[1,1,1,1,5]}  
    ];
    $scope.datamodal = [
            {"seccion":"Sección 3","valor":[1],"preguntas": ["pregunta3"]},
            {"seccion":"Sección 3","valor":[1],"preguntas": ["pregunta2"]},
            {"seccion":"Sección 3","valor":[1],"preguntas": ["pregunta1"]},
            {"seccion":"Sección 3","valor":[1],"preguntas": ["pregunta1"]},
            {"seccion":"Sección 3","valor":[5],"preguntas": ["pregunta1","pregunta2","pregunta3","pregunta4","pregunta5"]}
        
    ];
    
    $scope.datatest[0]["preguntas"]=[];
    for(i = 0; i <= $scope.datamodal.length-1; i++){
        $scope.datatest[0].preguntas.push($scope.datamodal[i]);
    } 
    
    principalBullet($scope.datatest, "#Q_S3", title=false, ind=true);

});

app.controller("TabsController", function($scope){
    this.tab=1;
    this.selectTab = function(tab){
        this.tab=tab;
    };   
});

app.controller("EGMGDI_bullet", function ($scope, $http) {
    $http.get("web/General/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#EGMGDI_bullet", title=true, ind=false);
    });
});

app.controller("EGMGDI_donut", function ($scope, $http) {
    $http.get("web/General/donut/donut.json").then(function(data){
        $scope.indi=data.data
        $scope.relleno = new Array();        
        $scope.relleno[0] = ($scope.indi[0]/$scope.indi[1])*100;
        $scope.relleno[1] = (1-($scope.indi[0]/$scope.indi[1]))*100;              
        donut($scope.indi, $scope.relleno, "#EGMGDI_donut");     
    });
});

app.controller("V_bullet", function ($scope, $http) {
    $http.get("web/Vicepr/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#V_bullet", title=false, ind=false);
    });
});

app.controller("V_donut", function ($scope, $http) {
    $http.get("web/Vicepr/donut/donut.json").then(function(data){
        $scope.indi=data.data
        $scope.relleno = new Array();        
        $scope.relleno[0] = ($scope.indi[0]/$scope.indi[1])*100;
        $scope.relleno[1] = (1-($scope.indi[0]/$scope.indi[1]))*100;               
        donut($scope.indi, $scope.relleno, "#V_donut");     
    });
});


app.controller("JyL_donut", function ($scope, $http) {
    $http.get("web/Jefes/donut/donut.json").then(function(data){
        $scope.indi=data.data
        $scope.relleno = new Array();        
        $scope.relleno[0] = ($scope.indi[0]/$scope.indi[1])*100;
        $scope.relleno[1] = (1-($scope.indi[0]/$scope.indi[1]))*100;                
        donut($scope.indi, $scope.relleno, "#JyL_donut");     
    });
});


app.controller("JyL_bullet", function ($scope, $http) {
    $http.get("web/Jefes/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet2($scope.data, "#JyL_bullet", title=false, ind=false);
    });
});

app.controller("ACI_donut", function ($scope, $http) {
    $http.get("web/Internacionales/donut/donut.json").then(function(data){
        $scope.indi=data.data
        $scope.relleno = new Array();        
        $scope.relleno[0] = ($scope.indi[0]/$scope.indi[1])*100;
        $scope.relleno[1] = (1-($scope.indi[0]/$scope.indi[1]))*100;                
        donut($scope.indi, $scope.relleno, "#ACI_donut");     
    });
});

app.controller("ACI_bullet", function ($scope, $http) {
    $http.get("web/Internacionales/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet2($scope.data, "#ACI_bullet", title=false, ind=false);
    });    
});

app.controller("ACN_donut", function ($scope, $http) {
    $http.get("web/Nacionales/donut/donut.json").then(function(data){
        $scope.indi=data.data
        $scope.relleno = new Array();        
        $scope.relleno[0] = ($scope.indi[0]/$scope.indi[1])*100;
        $scope.relleno[1] = (1-($scope.indi[0]/$scope.indi[1]))*100;               
        donut($scope.indi, $scope.relleno, "#ACN_donut");      
    });
});


app.controller("ACN_bullet", function ($scope, $http) {
    $http.get("web/Nacionales/bullet/bullet.json").then(function(data){
        $scope.data=data.data;
        principalBullet($scope.data, "#ACN_bullet", title=false, ind=false);
    });
});


