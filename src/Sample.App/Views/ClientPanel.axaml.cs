using Avalonia.Controls;
using Sample.App.ViewModels;

namespace Sample.App.Views;

public partial class ClientPanel : UserControl
{
    public ClientPanel() => InitializeComponent();

    /// <summary>Grid columns are not in the visual tree, so the columns tagged "owners" follow the panel's owner count here.</summary>
    protected override void OnDataContextChanged(EventArgs e)
    {
        base.OnDataContextChanged(e);
        bool many = DataContext is ClientPanelViewModel { ManyOwners: true };
        foreach (DataGridColumn column in new[] { ItemsGrid, CategoriesGrid, LogGrid }.SelectMany(g => g.Columns).Where(c => Equals(c.Tag, "owners")))
            column.IsVisible = many;
    }
}
