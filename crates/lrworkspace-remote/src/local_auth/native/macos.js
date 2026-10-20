ObjC.import('Cocoa');
ObjC.bindFunction('kill',['int',['int','int']]);
function postLocal(form,payload) {
    var task=$.NSTask.alloc.init;task.launchPath=form.callback_helper;task.arguments=['--private-submit'];
    var input=$.NSPipe.pipe,output=$.NSPipe.pipe,errors=$.NSPipe.pipe;
    task.standardInput=input;task.standardOutput=output;task.standardError=errors;task.launch;
    input.fileHandleForWriting.writeData($(JSON.stringify({url:form.callback_url,certificate:form.callback_certificate,body:JSON.stringify(payload)})).dataUsingEncoding($.NSUTF8StringEncoding));
    input.fileHandleForWriting.closeFile;output.fileHandleForReading.readDataToEndOfFile;task.waitUntilExit;
    return Number(task.terminationStatus)===0;
}
function run() {
    try {
        var data=$.NSFileHandle.fileHandleWithStandardInput.readDataToEndOfFile;
        var form=JSON.parse(ObjC.unwrap($.NSString.alloc.initWithDataEncoding(data,$.NSUTF8StringEncoding)));
        // SharingType.none is a legacy value on current macOS. Keep a genuine
        // NSSecureTextField and provide no unsafe clear-text preview.
        if(form.validate_only) {
            var check=$.NSSecureTextField.alloc.initWithFrame($.NSMakeRect(0,0,100,25));
            if(form.callback_url&&!postLocal(form,{host:'192.0.2.1',port:2222,user:'fixture',name:'fixture',remote_work_dir:'/work',node_mode:'submit_only',socks_proxy:'',authorized:true,password:'ui-validation-only'}))return 'cancelled';
            return 'validated';
        }
        var app=$.NSApplication.sharedApplication;
        app.setActivationPolicy($.NSApplicationActivationPolicyAccessory);
        app.activateIgnoringOtherApps(true);
        var alert=$.NSAlert.alloc.init;
        var closed=false;
        ObjC.registerSubclass({name:'LrwLoginParentWatch',methods:{'tick:':{types:['void',['id']],implementation:function(timer){if($.kill(Number(form.parent_pid),0)!==0){closed=true;app.abortModal;}}}}});
        var watcher=$.LrwLoginParentWatch.alloc.init;
        var timer=$.NSTimer.timerWithTimeIntervalTargetSelectorUserInfoRepeats(1,watcher,'tick:',null,true);
        $.NSRunLoop.mainRunLoop.addTimerForMode(timer,$.NSModalPanelRunLoopMode);
        alert.messageText=form.confirm_prompt?'LRW: confirm server identity':'LRW - 本机私密登录';
        if(form.confirm_prompt) {
            alert.informativeText=form.confirm_prompt;
            alert.addButtonWithTitle('信任并继续');alert.addButtonWithTitle('取消');
            var accepted=Number(alert.runModal)===1000;timer.invalidate;return accepted&&!closed?'yes':'no';
        }
        alert.informativeText='密码仅在本机内存中使用，不发送给模型。macOS 保持密码遮蔽；普通窗口不能保证阻止所有屏幕采集。';
        alert.addButtonWithTitle('连接并授权');alert.addButtonWithTitle('取消');
        var view=$.NSView.alloc.initWithFrame($.NSMakeRect(0,0,520,410));
        function field(label,value,y,secure) {
            var title=$.NSTextField.labelWithString(label);title.frame=$.NSMakeRect(0,y,155,25);view.addSubview(title);
            var box=(secure?$.NSSecureTextField:$.NSTextField).alloc.initWithFrame($.NSMakeRect(160,y,355,25));
            box.stringValue=value||'';box.editable=secure||!form.locked;view.addSubview(box);return box;
        }
        var host=field('服务器 IP / DNS',form.host,370,false);
        var port=field('端口',String(form.port),328,false);
        var user=field('SSH 用户名',form.user,286,false);
        var name=field('工作区名称',form.name,244,false);
        var work=field('远端工作目录',form.remote_work_dir,202,false);
        var proxy=field('SOCKS5 代理（可选）',form.socks_proxy,160,false);
        var label=$.NSTextField.labelWithString('节点用途');label.frame=$.NSMakeRect(0,118,155,25);view.addSubview(label);
        var role=$.NSPopUpButton.alloc.initWithFramePullsDown($.NSMakeRect(160,118,355,25),false);
        role.addItemsWithTitles(['集群登录节点：Slurm / PBS 提交','单服务器：直接运行']);
        role.selectItemAtIndex(form.node_mode==='single_server'?1:0);role.enabled=!form.node_mode_locked;view.addSubview(role);
        var password=field('密码（保护输入）','',76,true);
        var consent=$.NSButton.checkboxWithTitleTargetAction('允许本机 agent 按 SSH 账户权限访问此工作目录',null,null);
        consent.frame=$.NSMakeRect(0,20,515,36);view.addSubview(consent);alert.accessoryView=view;
        var answer=Number(alert.runModal);timer.invalidate;
        if(closed||answer!==1000)return 'cancelled';
        var payload={host:ObjC.unwrap(host.stringValue),port:Number(ObjC.unwrap(port.stringValue)),user:ObjC.unwrap(user.stringValue),
            name:ObjC.unwrap(name.stringValue),remote_work_dir:ObjC.unwrap(work.stringValue),socks_proxy:ObjC.unwrap(proxy.stringValue),
            node_mode:Number(role.indexOfSelectedItem)===0?'submit_only':'single_server',authorized:Number(consent.state)===1,
            password:ObjC.unwrap(password.stringValue)};
        if(!payload.host||!payload.user||!payload.password||payload.port<1||payload.port>65535||!payload.authorized)return 'cancelled';
        // The matching LRW helper receives the form on a private pipe and
        // verifies the pinned local TLS certificate before forwarding it.
        var submitted=postLocal(form,payload);
        password.stringValue='';payload.password='';
        return submitted?'submitted':'cancelled';
    }catch(error){return 'cancelled';}
}
