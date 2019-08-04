<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<html>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>welcome</title>
<style>
#bg{top; width:491px;height:365px}
.lo_input{ background-color:#1D2846; border:1px solid #fff; width:140px; height:23px; color:#fff;}
</style>
<SCRIPT language=JavaScript>
function checkform(){
	var operId = document.loginForm.operId.value;
	var pwd = document.loginForm.password.value;
	
	if (operId==""){		
		alert("请填写用户号");
		document.loginForm.operId.focus();		
		return false;		
	}
  if(!isNumberzs(document.loginForm.operId)){	
		alert("用户号必须是数字格式!");
		document.loginForm.operId.focus();
		return false;
	}
	if (operId.length !=8){
		alert("请正确填写用户号,长度必须是8位!"); 
		document.loginForm.operId.focus();
		return false;
	}
	if (pwd==""){		
		alert("请填写密码");    
		document.loginForm.password.focus();	
		return false;		
	}
  if(!isNumberzs(document.loginForm.password)){	
		alert("密码必须是数字格式!");
		document.loginForm.password.focus();
		return false;
	}

	if (pwd.length !=8){
		alert("密码长度必须是8位!"); 
		document.loginForm.password.focus();
		return false;
	}

	return true;
}
function setfocus() {
	document.loginForm.operId.focus();
  return;
}
function init(){
	if(window.parent.frames[0]!=undefined){
		parent.location.href='login.jsp';	
	}
}
</SCRIPT>
</head>
	<body onload="init()" style="background: url(images/login_bg.jpg) repeat-x; padding:150px 0 0 0;">
	<form action="login"  onsubmit="return checkform(this);">
	<table border="0" align="center" cellpadding="0" cellspacing="0" id="bg">
      <tr>
        <td valign="top"><table border="0" cellpadding="5" cellspacing="0" style="margin:75px 0 0 81px">
          <tr>
            <td>&nbsp;</td>
            <td colspan="2" class="font_w14" >welcome</td>
          </tr>
          <tr>
            <td class="font_write12">用户名:</td>
            <td><input name="username" type="text" maxlength="8" value="99999999" class="lo_input"></td>
             <td rowspan="2"><input type="image" src="images/btn_login.jpg" width="77" height="60" border="0"></td>
          </tr>
          <tr>
            <td class="font_write12">密码:</td>
            <td><input name="password" type="password" maxlength="8" value="99999999" class="lo_input"></td>
          </tr>
        </table>
          <table border="0" align="center" cellpadding="0" cellspacing="0"style="margin:96px 0 0 0">
          <tr>
            <td>@2009</td>
          </tr>
        </table></td>
      </tr>
    </table>
	</form>
	</body>
</html>