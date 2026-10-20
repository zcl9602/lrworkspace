param($inputData)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::InputEncoding = New-Object Text.UTF8Encoding($false)
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
try {
    Add-Type -AssemblyName System.Windows.Forms
    Add-Type -AssemblyName System.Drawing
    Add-Type -TypeDefinition @'
using System;
using System.Drawing;
using System.Runtime.InteropServices;
using System.Windows.Forms;
public class LrwPasswordPreview : Control {
    public TextBox PasswordBox;
    public bool Reveal;
    public LrwPasswordPreview() { Text=""; AccessibleName="Protected password preview"; }
    protected override void OnPaint(PaintEventArgs e) {
        base.OnPaint(e);
        e.Graphics.Clear(SystemColors.Window);
        if(Reveal && PasswordBox != null)
            e.Graphics.DrawString(PasswordBox.Text,Font,Brushes.Black,new RectangleF(4,4,Width-8,Height-8));
    }
    protected override AccessibleObject CreateAccessibilityInstance() { return new RedactedAccessibility(this); }
    private class RedactedAccessibility : ControlAccessibleObject {
        public RedactedAccessibility(Control owner):base(owner){}
        public override string Value { get {return "[REDACTED]";} set {} }
        public override string Name { get {return "Protected password preview";} set {} }
    }
}
public static class LrwCapture {
    [DllImport("user32.dll")] static extern bool SetWindowDisplayAffinity(IntPtr window,uint value);
    [DllImport("user32.dll")] static extern bool GetWindowDisplayAffinity(IntPtr window,out uint value);
    [DllImport("dwmapi.dll")] static extern int DwmIsCompositionEnabled(out bool enabled);
    public static bool Protect(IntPtr window) {
        try { bool composed; uint affinity; return DwmIsCompositionEnabled(out composed)==0 && composed &&
            SetWindowDisplayAffinity(window,0x11) && GetWindowDisplayAffinity(window,out affinity) && affinity==0x11; }
        catch {return false;}
    }
}
public static class LrwPrivatePost {
    public static void Send(string helper,string packet) {
        var settings=new System.Diagnostics.ProcessStartInfo(helper,"--private-submit");
        settings.UseShellExecute=false;settings.CreateNoWindow=true;
        settings.RedirectStandardInput=true;settings.RedirectStandardOutput=true;settings.RedirectStandardError=true;
        using(var process=System.Diagnostics.Process.Start(settings)) {
            var bytes=System.Text.Encoding.UTF8.GetBytes(packet);
            try {process.StandardInput.BaseStream.Write(bytes,0,bytes.Length);process.StandardInput.BaseStream.Flush();}
            finally {Array.Clear(bytes,0,bytes.Length);process.StandardInput.Close();}
            if(!process.WaitForExit(15000)){process.Kill();throw new InvalidOperationException("Private handoff timeout");}
            if(process.ExitCode!=0)throw new InvalidOperationException("Private handoff failed");
        }
    }
}
'@ -ReferencedAssemblies System.Windows.Forms,System.Drawing,Accessibility
    if ($inputData.validate_only) {
        $box=New-Object Windows.Forms.TextBox;$box.UseSystemPasswordChar=$true;$box.Text='ui-validation-only'
        $check=New-Object LrwPasswordPreview;$check.PasswordBox=$box;$check.Reveal=$true
        if($check.AccessibilityObject.Value -ne '[REDACTED]'){throw 'Unsafe accessibility preview'}
        $check.Dispose();$box.Clear();$box.Dispose()
        if($inputData.callback_url){
            $sample=@{host='192.0.2.1';port=2222;user='fixture';name='fixture';remote_work_dir='/work';node_mode='submit_only';socks_proxy='';authorized=$true;password='ui-validation-only'}|ConvertTo-Json -Compress
            $packet=@{url=$inputData.callback_url;certificate=$inputData.callback_certificate;body=$sample}|ConvertTo-Json -Compress
            [LrwPrivatePost]::Send([string]$inputData.callback_helper,$packet)
        }
        [Console]::Out.WriteLine('validated'); exit 0
    }
    $parentWatch=New-Object Windows.Forms.Timer;$parentWatch.Interval=1000
    $parentWatch.Add_Tick({try{$parent=[Diagnostics.Process]::GetProcessById([int]$inputData.parent_pid);if($parent.HasExited){[Environment]::Exit(1)}}catch{[Environment]::Exit(1)}})
    $parentWatch.Start()
    if ($inputData.confirm_prompt) {
        $answer = [Windows.Forms.MessageBox]::Show([string]$inputData.confirm_prompt,'LRW: confirm server identity',[Windows.Forms.MessageBoxButtons]::YesNo,[Windows.Forms.MessageBoxIcon]::Warning)
        if ($answer -eq [Windows.Forms.DialogResult]::Yes) {[Console]::Out.WriteLine('yes')} else {[Console]::Out.WriteLine('no')}
        exit 0
    }
    $form = New-Object Windows.Forms.Form
    $form.Text = 'LRW - 本机私密登录'
    $form.ClientSize = New-Object Drawing.Size(660,660)
    $form.StartPosition = 'CenterScreen'
    $form.Font = New-Object Drawing.Font('Segoe UI',10)
    $form.FormBorderStyle = 'FixedDialog'
    $form.MaximizeBox = $false
    $form.MinimizeBox = $false
    function AddLabel([string]$text,[int]$y) {
        $label = New-Object Windows.Forms.Label
        $label.Text=$text; $label.Location=New-Object Drawing.Point(18,$y); $label.Size=New-Object Drawing.Size(165,26)
        $form.Controls.Add($label)
    }
    function AddEntry([string]$text,[int]$y,[int]$limit) {
        $box=New-Object Windows.Forms.TextBox
        $box.Text=$text; $box.Location=New-Object Drawing.Point(190,$y); $box.Size=New-Object Drawing.Size(450,28); $box.MaxLength=$limit
        $box.ReadOnly=[bool]$inputData.locked; $form.Controls.Add($box); return $box
    }
    $notice=New-Object Windows.Forms.Label
    $notice.Text="在此窗口输入，不要把密码发送到聊天中。密码仅保留在当前 MCP 进程内存。`n本机防捕获是辅助保护，不能防范同权限程序、摄像或系统级监控。"
    $notice.Location=New-Object Drawing.Point(18,12);$notice.Size=New-Object Drawing.Size(620,65);$form.Controls.Add($notice)
    AddLabel '服务器 IP / DNS' 88; $hostBox=AddEntry ([string]$inputData.host) 86 253
    AddLabel '端口' 128; $portBox=AddEntry ([string]$inputData.port) 126 5
    AddLabel 'SSH 用户名' 168; $userBox=AddEntry ([string]$inputData.user) 166 128
    AddLabel '工作区名称' 208; $nameBox=AddEntry ([string]$inputData.name) 206 100
    AddLabel '远端工作目录' 248; $workBox=AddEntry ([string]$inputData.remote_work_dir) 246 4096
    AddLabel 'SOCKS5 代理（可选）' 288; $proxyBox=AddEntry ([string]$inputData.socks_proxy) 286 80
    AddLabel '节点用途' 328
    $role=New-Object Windows.Forms.ComboBox
    $role.Location=New-Object Drawing.Point(190,326);$role.Size=New-Object Drawing.Size(450,28);$role.DropDownStyle='DropDownList'
    [void]$role.Items.Add('集群登录节点：默认 Slurm / PBS 提交')
    [void]$role.Items.Add('单服务器：直接运行')
    $role.SelectedIndex=0;if($inputData.node_mode -eq 'single_server'){$role.SelectedIndex=1}
    $role.Enabled=-not [bool]$inputData.node_mode_locked;$form.Controls.Add($role)
    AddLabel '密码' 370
    $password=New-Object Windows.Forms.TextBox
    $password.Location=New-Object Drawing.Point(190,368);$password.Size=New-Object Drawing.Size(450,28)
    $password.UseSystemPasswordChar=$true;$password.MaxLength=1024;$password.AccessibleName='SSH password (protected)';$form.Controls.Add($password)
    $reveal=New-Object Windows.Forms.CheckBox
    $reveal.Text='在受保护区域显示明文预览';$reveal.Location=New-Object Drawing.Point(190,408);$reveal.Size=New-Object Drawing.Size(430,26);$reveal.Enabled=$false;$form.Controls.Add($reveal)
    $preview=New-Object LrwPasswordPreview
    $preview.Location=New-Object Drawing.Point(190,438);$preview.Size=New-Object Drawing.Size(450,36);$preview.PasswordBox=$password;$form.Controls.Add($preview)
    $reveal.Add_CheckedChanged({$preview.Reveal=$reveal.Checked;$preview.Invalidate()})
    $password.Add_TextChanged({$preview.Invalidate()})
    $protection=New-Object Windows.Forms.Label
    $protection.Text='检测到系统捕获保护后才允许明文预览。';$protection.Location=New-Object Drawing.Point(18,485);$protection.Size=New-Object Drawing.Size(620,28);$form.Controls.Add($protection)
    $form.Add_Shown({
        if([LrwCapture]::Protect($form.Handle)){$reveal.Enabled=$true;$protection.Text='系统捕获保护已启用；预览不向辅助功能接口提供密码。'}
        else{$protection.Text='系统捕获保护不可用：密码保持遮蔽，明文预览已禁用。'}
        $password.Focus()
    })
    $consent=New-Object Windows.Forms.CheckBox
    $consent.Text='连接并允许本机 agent 访问所列工作目录（按 SSH 账户权限）'
    $consent.Location=New-Object Drawing.Point(18,530);$consent.Size=New-Object Drawing.Size(620,30);$form.Controls.Add($consent)
    $connect=New-Object Windows.Forms.Button
    $connect.Text='连接并授权';$connect.Location=New-Object Drawing.Point(408,600);$connect.Size=New-Object Drawing.Size(112,34);$form.Controls.Add($connect)
    $cancel=New-Object Windows.Forms.Button
    $cancel.Text='取消';$cancel.Location=New-Object Drawing.Point(530,600);$cancel.Size=New-Object Drawing.Size(110,34);$cancel.DialogResult='Cancel';$form.Controls.Add($cancel);$form.CancelButton=$cancel
    $connect.Add_Click({
        try {
            $port=0
            if(-not [int]::TryParse($portBox.Text,[ref]$port) -or $port -lt 1 -or $port -gt 65535 -or -not $hostBox.Text -or -not $userBox.Text -or -not $workBox.Text.StartsWith('/') -or -not $password.Text -or -not $consent.Checked){
                [void][Windows.Forms.MessageBox]::Show('请填写有效连接信息、密码和工作目录，并确认访问授权。','LRW');return
            }
            $payload=@{host=$hostBox.Text;port=$port;user=$userBox.Text;name=$nameBox.Text;remote_work_dir=$workBox.Text;node_mode=$(if($role.SelectedIndex -eq 0){'submit_only'}else{'single_server'});socks_proxy=$proxyBox.Text;authorized=$true;password=$password.Text}|ConvertTo-Json -Compress
            $packet=@{url=$inputData.callback_url;certificate=$inputData.callback_certificate;body=$payload}|ConvertTo-Json -Compress
            [LrwPrivatePost]::Send([string]$inputData.callback_helper,$packet);$payload=$null;$packet=$null
            $password.Clear();$preview.Reveal=$false;$form.DialogResult='OK';$form.Close()
        }catch{[void][Windows.Forms.MessageBox]::Show('本机凭据传递未完成，请取消后重新发起登录。','LRW')}
    })
    $result=$form.ShowDialog()
    if($result -eq [Windows.Forms.DialogResult]::OK){[Console]::Out.WriteLine('submitted')}else{[Console]::Out.WriteLine('cancelled')}
    $password.Clear();$form.Dispose()
} catch {
    if ($inputData.validate_only) {[Console]::Error.WriteLine($_.Exception.ToString())}
    [Console]::Out.WriteLine('cancelled')
    exit 1
}
