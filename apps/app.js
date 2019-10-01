var app = angular.module('resumeWordCloud', ['ngSanitize', 'ngCsv']);
app.filter("toArray", function ()
{
	return function (obj)
	{
		var result = [];
		angular.forEach(obj, function (val, key)
		{
			val.KeyID = key
			result.push(val);
		});
		return result;
	};
});

app.filter("filterExtra", function ()
{
	return function (obj)
	{
		var result = {};
		angular.forEach(obj, function (val, key)
		{


			if(key != "file_path" && key != "text" && key != "text" && key != "extension"){
				var newValue;
				var newkey;
				try{
					if(typeof val == 'object' && Array.isArray(val)){
						newValue = val;
						newkey = key //+ " array"
												//console.log("Array "+newValue)
					} else if(typeof val == 'object' && !Array.isArray(val)){
						//console.log("dict "+ val[0])
						newValue = val;
						newkey = key //+ " obj"
					} else {
						val = val.replace(/^"(.+(?="$))"$/, '$1')
						//console.log(JSSON.parse(val))
						newValue = val;
						newkey = key //+ " string"

					}

				}catch(err){
					
				}
				

				if(newValue != ""){
					result[newkey] = newValue;
				}
			}
			
		});
		return result;
	};
});


app.filter("tocsvformat", function ()
{
	return function (obj, headermap)
	{
		var result = [];
		angular.forEach(obj, function (val, key)
		{
			updateValue = {};
			angular.forEach(headermap, function (v, k)
			{
				if (val[k] === undefined)
				{
					updateValue[v] = " "
				}
				else
				{
					updateValue[v] = val[k]
				}
			});


			result.push(updateValue);
		});
		return result;
	};
});
app.filter("filtertextRegex", function ()
{
	return function (input, regex)
	{
		var patt = new RegExp(regex, 'i');
		var out = [];
		for (var i = 0; i < input.length; i++)
		{
			if (patt.test(input[i].textData))
			{
				out.push(input[i]);
			}
		}
		return out;
	};
});
app.controller("myCtrl", function ($scope, $http, $timeout, $filter)
{

	$scope.albumBucketName = RESUME_BUCKET;
	$scope.showAlert = true;
	$scope.alertMessage = {
		message: 'Welcome to word cloud resume application'
	}
  function makeid(length) {
   var result           = '';
   var characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
   var charactersLength = characters.length;
   for ( var i = 0; i < length; i++ ) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
   }
   return result;
  }

	AWS.config.update(
	{
		region: BUCKET_REGION,
		credentials: new AWS.CognitoIdentityCredentials(
		{
			IdentityPoolId: IDENTIY_POOL_ID
		})
	});
	$scope.csvheaderMap = {
		"displaykey": "NAME",
		"email": "EMAIL",
		"phone": "PHONE",
		"tag": "JD Name",
		"score": "JD SCORE",
		"LastModified": "UPLOADED-ON",
		"matched": "JD Matched",
    "resumeImage": "Resume link",
    "wordcloudImage": "WordCloud Image link"
	}
	$scope.getCSVHeader = function (){
		return Object.values($scope.csvheaderMap);
	}

	$scope.dispData = {};
	$scope.imageData = {};
	$scope.textData = {};
	$scope.dispArray = [];
	$scope.jdlist = {
		"-all-":
		{
			"name": "-all-",
			"value": ""
		}
	};
	var s3 = new AWS.S3(
	{
		apiVersion: '2006-03-01',
		params:
		{
			Bucket: $scope.albumBucketName
		}
	});

	$scope.listResumeBucket = function (){
		$scope.alertMessage = {
			message: 'Loading Resumes from Server ....',
			showspinner: true
		}

		var params = {
			Prefix: 'resume',
			//MaxKeys: 2
		};
		s3.listObjects(params, function (err, data)
		{
			if (err) console.log(err, err.stack); // an error occurred
			else
			{
        var href = this.request.httpRequest.endpoint.href;
				$scope.bucketUrl = href + $scope.albumBucketName + '/';
				for (i = 0; i < data.Contents.length; i++)
				{
					key = data.Contents[i]["Key"]
					if (key.endsWith(".jd"))
					{
						continue;
					}
					if (key === "resume/")
						continue;
					key = key.substring(7);
					$scope.dispData[key] = data.Contents[i];
				}
				$scope.$apply(); // successful response
				$scope.dynamo();
			}
		})
	}


	$scope.hideAlert = function (){
		$scope.alertMessage = {
			message: 'Welcome to WordCloud Resume Application. Please choose a file and upload Resume. Current supported formats are pdf, doc, docx and image file'
		}
	}

	$scope.listResumeBucket();

	$scope.deletejd = function (key) {
		s3key = "resume/JD--" + key + ".jd"

		s3.deleteObject(
		{
			Key: s3key
		}, function (err, data)
		{
			if (err){
				$scope.alertMessage = {
					message: 'Error deleting the file'
				}
				$timeout($scope.hideAlert, 3000);
			} else {
				$scope.listResumeBucket();

			}

		});
	}

	$scope.deleteresume = function (key, dispKey) {
		$scope.alertMessage = {
			message: 'Deleting file from server: ' + key,
			showspinner: true
		}
		s3.deleteObject(
		{
			Key: key
		}, function (err, data)
		{
			if (err) {
				$scope.alertMessage = {
					message: 'Error deleting the file'
				}
				$timeout($scope.hideAlert, 3000);
			} else {
				delete $scope.dispData[dispKey];
				$scope.alertMessage = {
					message: 'File deleted Successfully'
				}
				$timeout($scope.hideAlert, 3000);
			}

		});
	}

	$scope.createjd = function () {
		var file = document.getElementById('jdfile').files[0]
		var fileName = "JD--" + $scope.jdName + ".jd";
		var albumPhotosKey = encodeURIComponent("resume") + '/';

		var photoKey = albumPhotosKey + fileName;
		s3.upload(
		{
			Key: photoKey,
			Body: file,
			ACL: 'public-read'
		}, function (err, data)
		{
			if (err) {
				$scope.alertMessage = {
					message: 'Error uploading the file'
				}
				$timeout($scope.hideAlert, 3000);

			}
			$scope.alertMessage = {
				message: 'File uploaded Successfully'
			}
			$('#jdmodal').modal('toggle');
			$scope.listResumeBucket();

		});
	}
	$scope.uploadFile = function () {

		if ($scope.selectedjd === undefined) {
			$scope.showjdalert = true;
			return;
		}
		tagname = $scope.selectedjd.name;
		if (tagname === "-all-") {
			$scope.showjdalert = true;
			return;
		}
		var file = document.getElementById('myresume').files[0]
		$scope.alertMessage = {
			message: 'Uploading new file to server: ' + file.name,
			showspinner: true
		}

		var fileName = "WCR-" + makeid(5)+file.name;
		var albumPhotosKey = encodeURIComponent("resume") + '/';
		var photoKey = albumPhotosKey + fileName;
		s3.upload(
		{
			Key: photoKey,
			Body: file,
			ACL: 'public-read',
			Metadata:
			{
				'tag': tagname,
			}
		}, function (err, data)
		{
			if (err) {
				$scope.alertMessage = {
					message: 'Error uploading the file'
				}
				$timeout($scope.hideAlert, 3000);

			}
			$scope.alertMessage = {
				message: 'File uploaded Successfully'
			}
			$scope.listResumeBucket();


		});

	};

	$scope.dynamo = function () {
		var ddb = new AWS.DynamoDB(
		{
			apiVersion: '2012-08-10'
		});

		var params = {
			ExpressionAttributeValues:
			{
				':topic':
				{
					S: 'resume'
				}
			},
			ProjectionExpression: 'resumekey, resumetext, imagekey, email, phone, metatag, score, matched, extraHeader',
			FilterExpression: 'begins_with (resumekey, :topic)',
			TableName: DYNAMO_DB_TABLE
		};

		ddb.scan(params, function (err, data) {
			if (err) {
				console.log("Error", err);
			}
			else {
				data.Items.forEach(function (element, index, array){
					key = element.resumekey.S.substring(7);
					$scope.textData[key] = element.resumetext.M;
					textString = "";
					angular.forEach(element.resumetext.M, function (value, key)
					{
						textString = textString + " " + key;
					});
					if (key.endsWith(".jd")){
						key = key.substring(4, key.lastIndexOf('.'))
						jd = {}
						jd['name'] = key;
						jd['text'] = textString;
						jd['value'] = key;
						jd['image'] = element.imagekey.S;
						$scope.jdlist[key] = jd;
					}	else if ($scope.dispData[key]) {
            var displayKey = key;
            if(key.startsWith("WCR-")){
              displayKey = displayKey.substring(9);
            }
            $scope.dispData[key]['displaykey'] = displayKey;
						$scope.dispData[key]['textData'] = textString;
						$scope.dispData[key]['matched'] = JSON.stringify(element.matched.M);
						$scope.dispData[key]['email'] = element.email.S;
						$scope.dispData[key]['phone'] = element.phone.S;
						if(element.extraHeader != undefined){
							var obj = JSON.parse(element.extraHeader.S)
							console.log(obj)
							$scope.dispData[key]['extraHeader'] = obj;
						}
            $scope.dispData[key]['resumeImage'] = $scope.bucketUrl + element.resumekey.S;
            $scope.dispData[key]['wordcloudImage'] = $scope.bucketUrl + element.imagekey.S;

						if (element.metatag === undefined) {
							$scope.dispData[key]['tag'] = 'BLANK'
						} else {
							$scope.dispData[key]['tag'] = element.metatag.S;
						}
						if (element.score != undefined) {
							$scope.dispData[key]['score'] = parseFloat(element.score.S) * 100;
            }
					}
				});

				if ($scope.selectedjd === undefined) {
					$scope.selectedjd = $scope.jdlist["-all-"];
				}
				else {
					$scope.selectedjd = $scope.jdlist[$scope.selectedjd.name];
				}

        $scope.alertMessage = {
          message: 'Data Refreshed'
        }
        $scope.$apply(); // successful response
        $timeout($scope.hideAlert, 3000);
			}
		})
	};


});
